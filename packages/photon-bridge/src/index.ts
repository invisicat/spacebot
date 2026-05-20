import { createInterface } from "node:readline";
import process from "node:process";

import { Spectrum, attachment, text } from "spectrum-ts";
import { imessage } from "spectrum-ts/providers/imessage";

type PhotonSpectrumApp = Awaited<ReturnType<typeof Spectrum>>;

/** Set in `main()` after `Spectrum()` resolves — required for outbound commands and inbound handling. */
let spectrumApp: PhotonSpectrumApp | undefined;

type JsonValue =
	| string
	| number
	| boolean
	| null
	| JsonValue[]
	| {[key: string]: JsonValue};

interface SidecarCommand {
	id: string;
	command:
		| "send_text"
		| "send_reply"
		| "send_file"
		| "add_reaction"
		| "remove_reaction"
		| "start_typing"
		| "stop_typing"
		| "health"
		| "shutdown";
	payload: {[key: string]: JsonValue};
}

interface SidecarResponse {
	type: "response";
	id: string;
	ok: boolean;
	error?: string;
}

interface SidecarReady {
	type: "ready";
	adapter_key: string;
}

interface SidecarLog {
	type: "log";
	level: "debug" | "warn" | "error";
	message: string;
}

interface SidecarInboundAttachment {
	filename: string;
	mime_type: string;
	/** HTTP(S) URL when the provider exposes a direct link (legacy / Slack-style). */
	url?: string;
	/** Raw bytes as base64 when Spectrum provides `read()` (iMessage images, audio, etc.). */
	data_base64?: string;
	size_bytes?: number;
}

interface SidecarInboundPayload {
	space_id: string;
	message_id: string;
	sender_id: string;
	sender_display_name?: string;
	text: string;
	timestamp?: string;
	mentions_or_replies_to_bot: boolean;
	is_dm: boolean;
	space_name?: string;
	server_name?: string;
	attachments: SidecarInboundAttachment[];
}

interface SidecarInbound {
	type: "inbound";
	payload: SidecarInboundPayload;
}

const adapterKey = process.env.PHOTON_ADAPTER_KEY ?? "photon";
const projectId = process.env.PHOTON_PROJECT_ID;
const projectSecret = process.env.PHOTON_PROJECT_SECRET;

if (!projectId || !projectSecret) {
	writeLog(
		"error",
		"missing PHOTON_PROJECT_ID or PHOTON_PROJECT_SECRET in environment",
	);
	process.exit(1);
}

const spaceCache = new Map<string, unknown>();
function emit(event: SidecarInbound | SidecarResponse | SidecarReady | SidecarLog) {
	process.stdout.write(`${JSON.stringify(event)}\n`);
}

function writeLog(level: SidecarLog["level"], message: string) {
	emit({
		type: "log",
		level,
		message,
	});
}

function commandError(id: string, error: unknown) {
	const message = error instanceof Error ? error.message : String(error);
	emit({
		type: "response",
		id,
		ok: false,
		error: message,
	});
}

function commandOk(id: string) {
	emit({
		type: "response",
		id,
		ok: true,
	});
}

function getString(value: unknown): string | undefined {
	return typeof value === "string" && value.trim().length > 0
		? value
		: undefined;
}

function getBoolean(value: unknown): boolean {
	return value === true;
}

function extractText(message: any): string {
	const content = message?.content;
	if (
		content &&
		typeof content === "object" &&
		content.type === "group" &&
		Array.isArray(content.items)
	) {
		return content.items
			.map((item: unknown) => extractText(item))
			.filter((part: string) => part.trim().length > 0)
			.join("\n");
	}
	if (content && typeof content === "object" && content.type === "text") {
		return getString(content.text) ?? "";
	}

	return (
		getString(message?.text) ??
		getString(content?.text) ??
		getString(message?.message?.text) ??
		""
	);
}

function extractId(value: any): string {
	return (
		getString(value?.id) ??
		getString(value?.messageId) ??
		getString(value?.message_id) ??
		crypto.randomUUID()
	);
}

function normalizeAttachments(value: any): SidecarInboundAttachment[] {
	if (!Array.isArray(value)) return [];

	return value
		.map((attachment): SidecarInboundAttachment | null => {
			const url =
				getString(attachment?.url) ??
				getString(attachment?.downloadUrl) ??
				getString(attachment?.download_url);
			if (!url) return null;

			return {
				filename: getString(attachment?.filename) ?? "attachment",
				mime_type:
					getString(attachment?.mimeType) ??
					getString(attachment?.mime_type) ??
					"application/octet-stream",
				url,
				size_bytes:
					typeof attachment?.sizeBytes === "number"
						? attachment.sizeBytes
						: typeof attachment?.size_bytes === "number"
							? attachment.size_bytes
							: undefined,
			};
		})
		.filter((attachment): attachment is SidecarInboundAttachment => attachment !== null);
}

async function spectrumAttachmentToSidecar(
	part: any,
): Promise<SidecarInboundAttachment | null> {
	if (typeof part?.read !== "function") {
		return null;
	}
	let buf: Buffer;
	try {
		buf = await part.read();
	} catch (error) {
		writeLog("warn", `failed to read spectrum attachment: ${String(error)}`);
		return null;
	}
	if (!buf?.length) {
		return null;
	}
	const filename = getString(part.name) ?? "attachment";
	const mime_type =
		getString(part.mimeType) ??
		getString(part.mime_type) ??
		"application/octet-stream";
	return {
		filename,
		mime_type,
		data_base64: buf.toString("base64"),
		size_bytes: typeof part.size === "number" ? part.size : buf.length,
	};
}

async function spectrumVoiceToSidecar(
	part: any,
): Promise<SidecarInboundAttachment | null> {
	if (typeof part?.read !== "function") {
		return null;
	}
	let buf: Buffer;
	try {
		buf = await part.read();
	} catch (error) {
		writeLog("warn", `failed to read spectrum voice message: ${String(error)}`);
		return null;
	}
	if (!buf?.length) {
		return null;
	}
	const mime_type =
		getString(part.mimeType) ??
		getString(part.mime_type) ??
		"audio/mpeg";
	const filename = getString(part.name) ?? "voice-message";
	return {
		filename,
		mime_type,
		data_base64: buf.toString("base64"),
		size_bytes: typeof part.size === "number" ? part.size : buf.length,
	};
}

async function collectAttachmentsFromContent(content: any): Promise<SidecarInboundAttachment[]> {
	if (!content || typeof content !== "object") {
		return [];
	}

	const type = content.type;
	if (type === "group" && Array.isArray(content.items)) {
		const results: SidecarInboundAttachment[] = [];
		for (const item of content.items) {
			results.push(...(await collectAttachmentsFromMessage(item)));
		}
		return results;
	}

	if (type === "reply" && content.content) {
		return collectAttachmentsFromContent(content.content);
	}

	if (type === "effect" && content.content) {
		return collectAttachmentsFromContent(content.content);
	}

	if (type === "attachment") {
		const attachment = await spectrumAttachmentToSidecar(content);
		return attachment ? [attachment] : [];
	}

	if (type === "voice") {
		const attachment = await spectrumVoiceToSidecar(content);
		return attachment ? [attachment] : [];
	}

	if (type === "richlink" && typeof content.cover === "function") {
		try {
			const cover = await content.cover();
			if (cover && typeof cover.read === "function") {
				const buf = await cover.read();
				if (buf?.length) {
					const mime_type =
						getString(cover.mimeType) ??
						getString(cover.mime_type) ??
						"image/jpeg";
					return [
						{
							filename: "link-preview",
							mime_type,
							data_base64: buf.toString("base64"),
							size_bytes: buf.length,
						},
					];
				}
			}
		} catch {
			/* link preview image is optional */
		}
		return [];
	}

	if (
		type === "contact" &&
		content.photo &&
		typeof content.photo.read === "function"
	) {
		try {
			const buf = await content.photo.read();
			if (buf?.length) {
				const mime_type =
					getString(content.photo.mimeType) ??
					getString(content.photo.mime_type) ??
					"image/jpeg";
				return [
					{
						filename: "contact-photo.jpg",
						mime_type,
						data_base64: buf.toString("base64"),
						size_bytes: buf.length,
					},
				];
			}
		} catch {
			return [];
		}
	}

	return [];
}

async function collectAttachmentsFromMessage(
	message: any,
): Promise<SidecarInboundAttachment[]> {
	if (!message) {
		return [];
	}
	if (typeof message === "object" && message.content) {
		return collectAttachmentsFromContent(message.content);
	}
	return collectAttachmentsFromContent(message);
}

async function extractInboundPayload(
	message: any,
): Promise<SidecarInboundPayload | null> {
	const spaceId =
		getString(message?.spaceId) ??
		getString(message?.space_id) ??
		getString(message?.space?.id);
	const senderId =
		getString(message?.senderId) ??
		getString(message?.sender_id) ??
		getString(message?.sender?.id);

	if (!spaceId || !senderId) {
		return null;
	}

	const legacyAttachments = normalizeAttachments(
		message?.attachments ?? message?.files ?? message?.media,
	);
	const spectrumAttachments = await collectAttachmentsFromMessage(message);

	const payload: SidecarInboundPayload = {
		space_id: spaceId,
		message_id: extractId(message),
		sender_id: senderId,
		sender_display_name:
			getString(message?.senderDisplayName) ??
			getString(message?.sender_display_name) ??
			getString(message?.sender?.name),
		text: extractText(message),
		timestamp:
			getString(message?.timestamp) ??
			(message?.timestamp instanceof Date
				? message.timestamp.toISOString()
				: undefined) ??
			getString(message?.createdAt) ??
			getString(message?.created_at),
		mentions_or_replies_to_bot:
			getBoolean(message?.mentionsOrRepliesToBot) ||
			getBoolean(message?.mentions_or_replies_to_bot) ||
			getBoolean(message?.mentionedAgent),
		is_dm:
			getBoolean(message?.isDm) ||
			getBoolean(message?.is_dm) ||
			getBoolean(message?.space?.isDm) ||
			message?.space?.type === "dm",
		space_name:
			getString(message?.spaceName) ??
			getString(message?.space_name) ??
			getString(message?.space?.name),
		server_name:
			getString(message?.serverName) ??
			getString(message?.server_name),
		attachments: [...spectrumAttachments, ...legacyAttachments],
	};

	const cachedSpace =
		message?.space ??
		(typeof message === "object" && message !== null && "space" in message
			? (message as { space?: unknown }).space
			: undefined);
	if (cachedSpace !== undefined && cachedSpace !== null) {
		spaceCache.set(spaceId, cachedSpace);
	}

	return payload;
}

function mergeTupleMessage(spaceObj: unknown, normalizedMessage: unknown): unknown {
	if (!normalizedMessage || typeof normalizedMessage !== "object") {
		return normalizedMessage;
	}
	const messageRecord = normalizedMessage as Record<string, unknown>;
	const spaceFallback =
		typeof spaceObj === "object" && spaceObj !== null ? spaceObj : undefined;

	return {
		...messageRecord,
		space: messageRecord.space ?? spaceFallback,
	};
}

async function resolveSpace(spaceId: string): Promise<any> {
	if (spaceCache.has(spaceId)) {
		return spaceCache.get(spaceId);
	}

	if (!spectrumApp) {
		throw new Error(`unable to resolve Photon space '${spaceId}' (Spectrum not started)`);
	}

	throw new Error(
		`unable to resolve Photon space '${spaceId}' — outbound sends need a cached space object from a prior inbound message in that chat`,
	);
}

async function sendText(space: any, body: string) {
	if (typeof space?.send === "function") {
		await space.send(body);
		return;
	}
	if (typeof space?.reply === "function") {
		await space.reply(body);
		return;
	}
	throw new Error("space object does not support send/reply");
}

async function sendReply(
	space: any,
	messageId: string | undefined,
	body: string,
) {
	const trimmedBody = body.trim();
	const trimmedMessageId = messageId?.trim();
	if (!trimmedBody) {
		return;
	}

	// Spectrum's `send()` resolves `string | ContentBuilder` only — not legacy `{ replyTo, text }`.
	if (!trimmedMessageId) {
		await sendText(space, body);
		return;
	}

	if (typeof space?.getMessage !== "function") {
		await sendText(space, body);
		return;
	}

	const targetMessage = await space.getMessage(trimmedMessageId);
	if (!targetMessage || typeof targetMessage.reply !== "function") {
		await sendText(space, body);
		return;
	}

	await targetMessage.reply(body);
}

async function addReaction(space: any, messageId: string, emoji: string, remove = false) {
	if (typeof space?.getMessage !== "function") {
		throw new Error("space object cannot resolve messages for reactions");
	}
	const message = await space.getMessage(messageId);
	if (!message || typeof message.react !== "function") {
		throw new Error(`message '${messageId}' not found or cannot react`);
	}
	if (remove) {
		writeLog(
			"warn",
			"Photon / spectrum-ts: remove_reaction is not implemented in the sidecar (tapback removal needs provider support).",
		);
		return;
	}
	await message.react(emoji);
}

async function setTyping(space: any, isTyping: boolean) {
	if (isTyping && typeof space?.startTyping === "function") {
		await space.startTyping();
		return;
	}
	if (!isTyping && typeof space?.stopTyping === "function") {
		await space.stopTyping();
	}
}

async function handleCommand(command: SidecarCommand) {
	const payload = command.payload ?? {};

	switch (command.command) {
		case "health":
			commandOk(command.id);
			return;
		case "shutdown":
			commandOk(command.id);
			process.exit(0);
		case "send_text": {
			const spaceId = getString(payload.space_id);
			const body = getString(payload.text) ?? "";
			if (!spaceId) throw new Error("send_text requires payload.space_id");
			const space = await resolveSpace(spaceId);
			await sendText(space, body);
			commandOk(command.id);
			return;
		}
		case "send_reply": {
			const spaceId = getString(payload.space_id);
			const body = getString(payload.text) ?? "";
			const replyTo = getString(payload.reply_to_message_id);
			if (!spaceId) throw new Error("send_reply requires payload.space_id");
			const space = await resolveSpace(spaceId);
			await sendReply(space, replyTo, body);
			commandOk(command.id);
			return;
		}
		case "send_file": {
			const spaceId = getString(payload.space_id);
			const fileName = getString(payload.filename) ?? "file";
			const mimeType = getString(payload.mime_type) ?? "application/octet-stream";
			const caption = getString(payload.caption);
			const base64Data = getString(payload.data_base64);
			const replyTo = getString(payload.reply_to_message_id);
			if (!spaceId) throw new Error("send_file requires payload.space_id");
			if (!base64Data) throw new Error("send_file requires payload.data_base64");

			const binaryData = Buffer.from(base64Data, "base64");
			const fileParts = [attachment(binaryData, { mimeType, name: fileName })];
			if (caption?.trim()) {
				fileParts.push(text(caption.trim()));
			}

			const space = await resolveSpace(spaceId);
			if (replyTo && typeof space?.getMessage === "function") {
				const targetMessage = await space.getMessage(replyTo);
				if (targetMessage && typeof targetMessage.reply === "function") {
					await targetMessage.reply(...fileParts);
					commandOk(command.id);
					return;
				}
			}
			if (typeof space?.send === "function") {
				await space.send(...fileParts);
			} else {
				throw new Error("space object does not support file sends");
			}
			commandOk(command.id);
			return;
		}
		case "add_reaction":
		case "remove_reaction": {
			const spaceId = getString(payload.space_id);
			const messageId = getString(payload.message_id);
			const emoji = getString(payload.emoji);
			if (!spaceId || !messageId || !emoji) {
				throw new Error("reaction command requires payload.space_id/message_id/emoji");
			}
			const space = await resolveSpace(spaceId);
			await addReaction(
				space,
				messageId,
				emoji,
				command.command === "remove_reaction",
			);
			commandOk(command.id);
			return;
		}
		case "start_typing":
		case "stop_typing": {
			const spaceId = getString(payload.space_id);
			if (!spaceId) throw new Error("typing command requires payload.space_id");
			const space = await resolveSpace(spaceId);
			await setTyping(space, command.command === "start_typing");
			commandOk(command.id);
			return;
		}
		default:
			throw new Error(`unsupported command '${command.command}'`);
	}
}

function wireInbound() {
	if (!spectrumApp) {
		throw new Error("Spectrum is not initialized");
	}

	const messages = spectrumApp.messages;
	if (!(messages && typeof messages[Symbol.asyncIterator] === "function")) {
		throw new Error("Spectrum messages stream is not async-iterable");
	}

	void (async () => {
		for await (const item of messages as AsyncIterable<unknown>) {
			let normalizedMessage = item;
			if (Array.isArray(item) && item.length >= 2) {
				const [spaceTuple, inbound] = item;
				normalizedMessage = mergeTupleMessage(spaceTuple, inbound);
			}
			const payload = await extractInboundPayload(normalizedMessage);
			if (!payload) continue;

			emit({
				type: "inbound",
				payload,
			});
		}
	})().catch((error) => {
		writeLog("error", `async inbound stream failed: ${String(error)}`);
	});
}

async function main() {
	spectrumApp = await Spectrum({
		projectId: projectId!,
		projectSecret: projectSecret!,
		providers: [imessage.config()],
	});

	wireInbound();
	emit({
		type: "ready",
		adapter_key: adapterKey,
	});

	const reader = createInterface({
		input: process.stdin,
		crlfDelay: Infinity,
	});

	reader.on("line", (line) => {
		if (!line.trim()) return;
		let command: SidecarCommand;
		try {
			command = JSON.parse(line);
		} catch (error) {
			writeLog("warn", `failed to parse command JSON: ${String(error)}`);
			return;
		}

		handleCommand(command).catch((error) => {
			commandError(command.id, error);
		});
	});

	reader.on("close", () => {
		process.exit(0);
	});

	process.on("SIGTERM", () => process.exit(0));
	process.on("SIGINT", () => process.exit(0));
}

main().catch((error) => {
	writeLog("error", `photon bridge fatal error: ${String(error)}`);
	process.exit(1);
});

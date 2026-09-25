/**
 * Capture helpers.
 *
 * Thin wrappers over `getUserMedia`/`getDisplayMedia` that exist for two
 * reasons: they turn the browser's permission errors into Beam's error model,
 * and they mean a developer publishing a camera never has to leave the SDK.
 */

import { BeamError, BeamUnsupportedError } from "../core/errors/index.js";

function requireMediaDevices(): MediaDevices {
  const devices = globalThis.navigator?.mediaDevices;
  if (!devices) {
    throw new BeamUnsupportedError(
      "navigator.mediaDevices is unavailable. Capture needs a secure context (HTTPS or localhost).",
    );
  }
  return devices;
}

/**
 * `getUserMedia` errors are `DOMException`s whose `name` is the only reliable
 * discriminator; the messages are browser-specific. Mapping them here means an
 * application can branch on Beam's codes instead.
 */
function toBeamMediaError(error: unknown, what: string): BeamError {
  const name = error instanceof Error ? error.name : "";
  switch (name) {
    case "NotAllowedError":
    case "SecurityError":
      return new BeamError(`Permission to capture ${what} was denied.`, {
        code: "permission_denied",
        retryable: false,
        cause: error,
      });
    case "NotFoundError":
    case "OverconstrainedError":
      return new BeamError(`No ${what} device matched the requested constraints.`, {
        code: "not_found",
        retryable: false,
        cause: error,
      });
    case "NotReadableError":
      return new BeamError(`The ${what} device is already in use by another application.`, {
        code: "conflict",
        retryable: false,
        cause: error,
      });
    default:
      return new BeamError(`Could not capture ${what}.`, { code: "internal", retryable: false, cause: error });
  }
}

/** Camera and microphone. */
export async function camera(constraints: MediaStreamConstraints = { audio: true, video: true }): Promise<MediaStream> {
  try {
    return await requireMediaDevices().getUserMedia(constraints);
  } catch (error) {
    throw toBeamMediaError(error, "camera or microphone");
  }
}

/** Microphone only. */
export async function microphone(constraints: MediaTrackConstraints | boolean = true): Promise<MediaStream> {
  return camera({ audio: constraints, video: false });
}

/** Screen, window, or tab, with audio when the browser and user allow it. */
export async function screen(options: DisplayMediaStreamOptions = { video: true, audio: true }): Promise<MediaStream> {
  const devices = requireMediaDevices();
  if (typeof devices.getDisplayMedia !== "function") {
    throw new BeamUnsupportedError("This browser does not support screen capture.");
  }
  try {
    return await devices.getDisplayMedia(options);
  } catch (error) {
    throw toBeamMediaError(error, "screen");
  }
}

/** Stops every track. Without this the camera light stays on after `leave()`. */
export function stopStream(stream: MediaStream | undefined): void {
  for (const track of stream?.getTracks() ?? []) track.stop();
}

export const media = { camera, microphone, screen, stopStream };

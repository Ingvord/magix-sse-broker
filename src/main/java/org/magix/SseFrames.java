package org.magix;

import java.nio.charset.StandardCharsets;

/** Serialises Server-Sent Events frames. Callers must pass single-line field values. */
final class SseFrames {

    private static final byte[] KEEPALIVE = ": keepalive\n\n".getBytes(StandardCharsets.UTF_8);

    private SseFrames() {
    }

    /** A comment-only frame: ignored by clients, but it is traffic for proxies and a first byte. */
    static byte[] keepalive() {
        return KEEPALIVE;
    }

    static byte[] event(String name, byte[] jsonData) {
        var head = ("event: " + name + "\ndata: ").getBytes(StandardCharsets.UTF_8);
        var frame = new byte[head.length + jsonData.length + 2];
        System.arraycopy(head, 0, frame, 0, head.length);
        System.arraycopy(jsonData, 0, frame, head.length, jsonData.length);
        frame[frame.length - 2] = '\n';
        frame[frame.length - 1] = '\n';
        return frame;
    }

    /** Channel names become the SSE event name, so line breaks would let a client forge frames. */
    static boolean isValidName(String name) {
        return name.indexOf('\n') < 0 && name.indexOf('\r') < 0;
    }
}

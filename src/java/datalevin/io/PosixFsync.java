package datalevin.io;

import java.io.FileDescriptor;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;

/**
 * WAL segment sync helpers.
 *
 * We avoid FileChannel.force() for default fsync paths on macOS because JDK's
 * FileDispatcherImpl force path may route through F_FULLFSYNC first.
 */
public final class PosixFsync {

    private static final Field FILE_CHANNEL_FD_FIELD = initFileChannelFdField();
    private static final boolean IS_MACOS = initIsMacOS();

    private static boolean initIsMacOS() {
        String os = System.getProperty("os.name", "");
        return os.toLowerCase().contains("mac");
    }

    private static Field initFileChannelFdField() {
        try {
            Class<?> cls = Class.forName("sun.nio.ch.FileChannelImpl");
            Field f = cls.getDeclaredField("fd");
            f.setAccessible(true);
            return f;
        } catch (Exception e) {
            return null;
        }
    }

    private PosixFsync() {
    }

    public static boolean available() {
        return FILE_CHANNEL_FD_FIELD != null;
    }

    private static FileDescriptor fileDescriptor(FileChannel ch) {
        if (FILE_CHANNEL_FD_FIELD == null) {
            return null;
        }
        try {
            Object v = FILE_CHANNEL_FD_FIELD.get(ch);
            if (v instanceof FileDescriptor) {
                return (FileDescriptor) v;
            }
        } catch (IllegalAccessException e) {
            return null;
        }
        return null;
    }

    /**
     * Force file data and metadata with fsync(2) semantics.
     */
    public static void fsync(FileChannel ch) throws IOException {
        FileDescriptor fd = fileDescriptor(ch);
        if (fd != null) {
            fd.sync();
        } else {
            // Fallback when fd reflection is unavailable.
            ch.force(false);
        }
    }

    /**
     * Force file data with fdatasync(2)-like semantics.
     *
     * Java does not expose a distinct fdatasync call.
     *
     * On macOS, avoid FileChannel.force(false) because JDK may route through
     * F_FULLFSYNC; use fsync path instead.
     * On non-macOS, use force(false) to approximate data-focused flush.
     */
    public static void fdatasync(FileChannel ch) throws IOException {
        if (IS_MACOS) {
            fsync(ch);
        } else {
            ch.force(false);
        }
    }

    /**
     * Extra-durable sync (SQLite-style "extra").
     *
     * This keeps using FileChannel.force(true), which on macOS may route
     * through F_FULLFSYNC.
     */
    public static void fullsync(FileChannel ch) throws IOException {
        ch.force(true);
    }
}

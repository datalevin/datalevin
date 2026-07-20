package datalevin.utl;

import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/** Buffered CSV row iterator with Datalevin's backslash-escaped quote handling. */
public final class CSVReader implements Iterator<List<String>> {
    private static final int BUFFER_SIZE = 64 * 1024;

    private final Reader reader;
    private final char separator;
    private final char quote;
    private final char[] buffer = new char[BUFFER_SIZE];

    private int position;
    private int limit;
    private boolean skipLineFeed;
    private boolean finished;
    private boolean prepared;
    private List<String> next;

    public CSVReader(Reader reader) {
        this(reader, ',', '"');
    }

    public CSVReader(Reader reader, char separator, char quote) {
        this.reader = reader;
        this.separator = separator;
        this.quote = quote;
    }

    @Override
    public boolean hasNext() {
        prepare();
        return next != null;
    }

    @Override
    public List<String> next() {
        prepare();
        if (next == null) throw new NoSuchElementException();
        List<String> record = next;
        next = null;
        prepared = false;
        return record;
    }

    private void prepare() {
        if (prepared) return;
        next = readRecord();
        prepared = true;
    }

    private List<String> readRecord() {
        if (finished) return null;

        ArrayList<String> record = new ArrayList<>(12);
        StringBuilder field = new StringBuilder(64);
        boolean quoted = false;
        boolean escaped = false;
        boolean started = false;

        while (true) {
            int value = read();
            if (value == -1) {
                finished = true;
                if (!started && record.isEmpty() && field.length() == 0) return null;
                record.add(field.toString());
                return record;
            }

            char c = (char) value;
            if (skipLineFeed) {
                skipLineFeed = false;
                if (c == '\n') continue;
            }
            started = true;

            if (c == separator) {
                if (quoted) {
                    field.append(c);
                } else {
                    record.add(field.toString());
                    field.setLength(0);
                }
                escaped = false;
            } else if (c == quote) {
                if (quoted) {
                    if (escaped) {
                        field.append(c);
                    } else {
                        quoted = false;
                    }
                } else if (field.length() == 0) {
                    quoted = true;
                } else {
                    field.append(c);
                }
                escaped = false;
            } else if (c == '\n') {
                if (quoted) {
                    field.append(c);
                    escaped = false;
                } else {
                    record.add(field.toString());
                    return record;
                }
            } else if (c == '\r') {
                if (quoted) {
                    field.append(c);
                    escaped = false;
                } else {
                    record.add(field.toString());
                    skipLineFeed = true;
                    return record;
                }
            } else if (c == '\\' && quoted) {
                escaped = true;
            } else {
                field.append(c);
                escaped = false;
            }
        }
    }

    private int read() {
        if (position == limit) {
            try {
                do {
                    limit = reader.read(buffer, 0, buffer.length);
                } while (limit == 0);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            position = 0;
            if (limit == -1) return -1;
        }
        return buffer[position++];
    }
}

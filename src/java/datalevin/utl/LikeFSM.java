package datalevin.utl;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Immutable, case-sensitive SQL-style LIKE matcher. Wildcards operate on Unicode
 * code points; matching uses a percent checkpoint instead of a byte DFA whose
 * fallback states cannot represent overlapping wildcard alternatives.
 */
public class LikeFSM {
    private static final int ONE = -1;
    private static final int MANY = -2;
    private static final char DEFAULT_ESCAPE = '!';
    private static final int EXACT = 0;
    private static final int PREFIX = 1;
    private static final int SUFFIX = 2;
    private static final int CONTAINS = 3;
    private static final int SEGMENTS = 4;
    private static final int WILDCARDS = 5;

    private final int[] pattern;
    private final int mode;
    private final String literal;
    private final String[] segments;

    public LikeFSM(String pattern, char escapeCharacter) {
        this.pattern = compile(pattern, escapeCharacter);
        int n = this.pattern.length;
        boolean leading = n != 0 && this.pattern[0] == MANY;
        boolean trailing = n != 0 && this.pattern[n - 1] == MANY;
        int from = leading ? 1 : 0;
        int to = trailing && n > from ? n - 1 : n;
        boolean simple = true;
        StringBuilder text = new StringBuilder();
        for (int i = from; i < to; i++) {
            if (this.pattern[i] < 0) {
                simple = false;
                break;
            }
            text.appendCodePoint(this.pattern[i]);
        }
        literal = simple ? text.toString() : null;
        if (simple) {
            mode = (leading ? SUFFIX : EXACT) + (trailing ? PREFIX : EXACT);
            segments = null;
        } else {
            boolean hasOne = false;
            for (int token : this.pattern) {
                if (token == ONE) {
                    hasOne = true;
                    break;
                }
            }
            mode = hasOne ? WILDCARDS : SEGMENTS;
            segments = hasOne ? null : literalSegments(this.pattern);
        }
    }

    public LikeFSM(String pattern) {
        this(pattern, DEFAULT_ESCAPE);
    }

    /** Retain the UTF-8 byte-array entry points for existing Java callers. */
    public LikeFSM(byte[] pattern, char escapeCharacter) {
        this(new String(pattern, StandardCharsets.UTF_8), escapeCharacter);
    }

    public LikeFSM(byte[] pattern) {
        this(pattern, DEFAULT_ESCAPE);
    }

    private static void validateEscape(int escaped, char escape) {
        if (escaped != '%' && escaped != '_' && escaped != escape) {
            throw new IllegalStateException(
                    "Can only escape %, _, and escape character itself in like pattern");
        }
    }

    public static boolean isValid(String pattern, char escape) {
        for (int i = 0; i < pattern.length();) {
            int cp = pattern.codePointAt(i);
            i += Character.charCount(cp);
            if (cp == escape && i < pattern.length()) {
                int escaped = pattern.codePointAt(i);
                validateEscape(escaped, escape);
                i += Character.charCount(escaped);
            }
        }
        return true;
    }

    public static boolean isValid(byte[] pattern, char escape) {
        return isValid(new String(pattern, StandardCharsets.UTF_8), escape);
    }

    private static int[] compile(String source, char escape) {
        int[] tokens = new int[source.length()];
        int size = 0;
        for (int i = 0; i < source.length();) {
            int cp = source.codePointAt(i);
            i += Character.charCount(cp);
            if (cp == escape) {
                // Preserve the established trailing-escape behavior: it adds
                // no literal. An escaped wildcard remains a literal token.
                if (i == source.length()) break;
                cp = source.codePointAt(i);
                validateEscape(cp, escape);
                i += Character.charCount(cp);
            } else if (cp == '%') {
                if (size != 0 && tokens[size - 1] == MANY) continue;
                cp = MANY;
            } else if (cp == '_') {
                cp = ONE;
            }
            tokens[size++] = cp;
        }
        return size == tokens.length ? tokens : Arrays.copyOf(tokens, size);
    }

    private static String[] literalSegments(int[] pattern) {
        ArrayList<String> segments = new ArrayList<>();
        StringBuilder text = new StringBuilder();
        for (int cp : pattern) {
            if (cp == MANY) {
                if (text.length() != 0) {
                    segments.add(text.toString());
                    text.setLength(0);
                }
            } else {
                text.appendCodePoint(cp);
            }
        }
        if (text.length() != 0) segments.add(text.toString());
        return segments.toArray(new String[0]);
    }

    private boolean matchSegments(String text) {
        int first = 0;
        int last = segments.length - 1;
        int start = 0;
        int end = text.length();
        if (pattern[0] != MANY) {
            String head = segments[first++];
            if (!text.startsWith(head)) return false;
            start = head.length();
        }
        if (pattern[pattern.length - 1] != MANY) {
            String tail = segments[last--];
            if (!text.endsWith(tail)) return false;
            end -= tail.length();
        }
        if (start > end) return false;
        for (int i = first; i <= last; i++) {
            String segment = segments[i];
            int at = text.indexOf(segment, start);
            if (at < 0 || at + segment.length() > end) return false;
            start = at + segment.length();
        }
        return true;
    }

    public boolean match(String text) {
        // Most indexed string filters are literals, prefixes, suffixes, or
        // contains tests. Keep those on the JVM's optimized String operations.
        switch (mode) {
            case EXACT: return text.equals(literal);
            case PREFIX: return text.startsWith(literal);
            case SUFFIX: return text.endsWith(literal);
            case CONTAINS: return text.contains(literal);
            case SEGMENTS: return matchSegments(text);
            default: break;
        }
        int p = 0;
        int t = 0;
        int percent = -1;
        int retry = 0;
        while (t < text.length()) {
            int cp = text.codePointAt(t);
            if (p < pattern.length && (pattern[p] == ONE || pattern[p] == cp)) {
                p++;
                t += Character.charCount(cp);
            } else if (p < pattern.length && pattern[p] == MANY) {
                percent = p++;
                if (p == pattern.length) return true;
                retry = t;
            } else if (percent != -1) {
                // Retry the suffix after the latest percent, consuming one
                // more whole character. No recursive/backtracking stack grows.
                retry += Character.charCount(text.codePointAt(retry));
                t = retry;
                p = percent + 1;
            } else {
                return false;
            }
        }
        while (p < pattern.length && pattern[p] == MANY) p++;
        return p == pattern.length;
    }

    public boolean match(byte[] text) {
        return match(new String(text, StandardCharsets.UTF_8));
    }
}

package eu.fbk.dkm.internal.util;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.CharStreams;

public final class MacroExpander implements Function<Object, String>
{

    private static final int MAX_NESTING = 10;

    private final char macroChar;

    private final char openChar;

    private final char closeChar;

    private final char separatorChar;

    private final Map<Macro, List<Object>> macroAtoms;

    private final Map<String, List<Macro>> macroIndex;

    private final Pattern pattern;

    public MacroExpander(final Iterable<Macro> macros)
    {
        this(macros, '#', '(', ')', ',');
    }

    public MacroExpander(final Iterable<Macro> macros, final char macroChar, final char openChar,
            final char closeChar, final char separatorChar)
    {
        Preconditions.checkArgument(macroChar != openChar && macroChar != closeChar
                && macroChar != separatorChar && openChar != closeChar
                && openChar != separatorChar && closeChar != separatorChar);

        this.macroChar = macroChar;
        this.openChar = openChar;
        this.closeChar = closeChar;
        this.separatorChar = separatorChar;

        this.pattern = Pattern.compile("\\" + macroChar + "([a-zA-Z_][a-zA-z_0-9]*)");

        final ImmutableMap.Builder<Macro, List<Object>> macroAtomsBuilder = ImmutableMap.builder();
        final Map<String, ImmutableList.Builder<Macro>> tempMap = Maps.newHashMap();
        for (final Macro macro : macros) {
            macroAtomsBuilder.put(macro, Lists.newArrayList());
            ImmutableList.Builder<Macro> listBuilder = tempMap.get(macro.getName());
            if (listBuilder == null) {
                listBuilder = ImmutableList.builder();
                tempMap.put(macro.getName(), listBuilder);
            }
            listBuilder.add(macro);
        }
        final ImmutableMap.Builder<String, List<Macro>> macroIndexBuilder = ImmutableMap.builder();
        for (final Map.Entry<String, ImmutableList.Builder<Macro>> entry : tempMap.entrySet()) {
            macroIndexBuilder.put(entry.getKey(), entry.getValue().build());
        }
        this.macroAtoms = macroAtomsBuilder.build();
        this.macroIndex = macroIndexBuilder.build();

        preprocess();
    }

    private void preprocess()
    {
        final Map<Macro, String> templates = Maps.newHashMap();
        for (final Macro macro : this.macroAtoms.keySet()) {
            templates.put(macro, macro.getTemplate());
        }

        boolean done = false;
        for (int i = 0; i < MAX_NESTING && !done; ++i) {
            for (final Macro macro : templates.keySet()) {
                final String template = templates.get(macro);
                final List<Object> atoms = this.macroAtoms.get(macro);
                atoms.clear();
                if (macro.isConstant()) {
                    atoms.add(template);
                } else {
                    final Matcher matcher = this.pattern.matcher(template);
                    int start = 0;
                    while (matcher.find()) {
                        final int argIndex = macro.getArgs().indexOf(matcher.group(1));
                        if (argIndex >= 0) {
                            if (matcher.start() > start) {
                                atoms.add(template.substring(start, matcher.start()));
                            }
                            atoms.add(argIndex);
                            start = matcher.end();
                        }
                    }
                    if (start < template.length()) {
                        atoms.add(template.substring(start));
                    }
                }
            }

            done = true;
            for (final Macro macro : Lists.newArrayList(templates.keySet())) {
                final String oldTemplate = templates.get(macro);
                final StringBuilder builder = new StringBuilder();
                try {
                    expand(oldTemplate, ImmutableSet.copyOf(macro.getArgs()), builder);
                } catch (final IOException ex) {
                    throw new Error("Unexpected exception", ex);
                }
                final String newTemplate = builder.toString();
                if (newTemplate.equals(oldTemplate)) {
                    templates.remove(macro);
                } else {
                    templates.put(macro, newTemplate);
                    done = false;
                }
            }
        }

        if (!done) {
            throw new IllegalArgumentException("Invalid macros: either there are circular "
                    + "definitions or there are dependency chains among macros of length "
                    + "greater than " + MAX_NESTING);
        }
    }

    private void expand(final String string, final Set<String> undefinedConstants,
            final Appendable output) throws IOException
    {
        final List<String> argValues = Lists.newArrayList();
        final Matcher matcher = this.pattern.matcher(string);
        int start = 0;

        while (matcher.find(start)) {
            output.append(string.substring(start, matcher.start()));

            final String name = matcher.group(1);
            final List<Macro> macros = this.macroIndex.get(name);
            if (macros == null) {
                if (!undefinedConstants.contains(name)) {
                    throw new IllegalArgumentException("Undefined macro/arg '" + name + "' in:\n"
                            + string);
                } else {
                    output.append(matcher.group());
                    start = matcher.end();
                }
            } else {
                if (macros.size() > 1 || !macros.get(0).getArgs().isEmpty()) {
                    start = extractArgs(string, matcher.end(), argValues);
                }
                Macro macro = null;
                for (final Macro m : macros) {
                    if (m.getArgs().size() == argValues.size() || //
                            macro == null && m.getArgs().isEmpty()) {
                        macro = m;
                    }
                }
                if (macro == null) {
                    throw new IllegalArgumentException("Undefined macro '" + name + "/"
                            + argValues.size() + "' in:\n" + string);
                }
                for (final Object atom : this.macroAtoms.get(macro)) {
                    if (atom instanceof String) {
                        output.append((String) atom);
                    } else {
                        expand(argValues.get((Integer) atom), undefinedConstants, output);
                    }
                }
                start = macro.getArgs().isEmpty() ? matcher.end() : start;
            }
        }

        output.append(string.substring(start, string.length()));
    }

    private int extractArgs(final String string, final int offset, final List<String> argValues)
    {
        argValues.clear();

        final int length = string.length();
        int start = -1;
        int level = 0;

        for (int i = offset; i < length; ++i) {
            final char c = string.charAt(i);
            if (c == this.openChar) {
                ++level;
                if (level == 1) {
                    start = i + 1;
                }
            } else if (c == this.closeChar && level > 0) {
                --level;
                if (level == 0) {
                    argValues.add(extractArg(string, start, i));
                    return i + 1;
                }
            } else if (c == this.separatorChar && level == 1) {
                argValues.add(extractArg(string, start, i));
                start = i + 1;

            } else if (level == 0) {
                return i;
            }
        }

        return length;
    }

    private String extractArg(final String string, final int start, final int end)
    {
        int s = start;
        while (s < end && Character.isWhitespace(string.charAt(s))) {
            ++s;
        }
        int e = end;
        while (e > s && Character.isWhitespace(string.charAt(e - 1))) {
            --e;
        }
        if (e - s >= 2 && string.charAt(s) == this.openChar
                && string.charAt(e - 1) == this.closeChar) {
            ++s;
            --e;
        }
        return string.substring(s, e);
    }

    public char getMacroChar()
    {
        return this.macroChar;
    }

    public char getOpenChar()
    {
        return this.openChar;
    }

    public char getCloseChar()
    {
        return this.closeChar;
    }

    public char getSeparatorChar()
    {
        return this.separatorChar;
    }

    public Set<Macro> getMacros()
    {
        return this.macroAtoms.keySet();
    }

    public Set<Macro> getMacros(final String name)
    {
        Preconditions.checkNotNull(name);

        final List<Macro> macros = this.macroIndex.get(name);
        return macros == null ? Collections.<Macro>emptySet() : ImmutableSet.copyOf(macros);
    }

    @Nullable
    public Macro getMacro(final String name, final int numArgs)
    {
        Preconditions.checkNotNull(name);
        Preconditions.checkArgument(numArgs >= 0);

        final List<Macro> macros = this.macroIndex.get(name);
        if (macros != null) {
            for (final Macro macro : macros) {
                if (macro.getArgs().size() == numArgs) {
                    return macro;
                }
            }
        }

        return null;
    }

    @Override
    @Nullable
    public String apply(@Nullable final Object object)
    {
        if (object == null) {
            return null;
        } else {
            final StringBuilder builder = new StringBuilder();
            apply(object.toString(), builder);
            return builder.toString();
        }
    }

    public void apply(@Nullable final Object object, final StringBuilder builder)
    {
        try {
            apply(object.toString(), (Appendable) builder);
        } catch (final IOException ex) {
            throw new Error("Unexpected exception: " + ex.getMessage(), ex);
        }
    }

    public void apply(@Nullable final Object object, final Appendable output) throws IOException
    {
        if (object == null) {
            output.append("null"); // as by 'Appendable.append' specification
        } else {
            expand(object.toString(), Collections.<String>emptySet(), output);
        }
    }

    @Nullable
    public Reader transform(@Nullable final Reader reader) throws IOException
    {
        if (reader == null) {
            return null;
        }

        // XXX: inefficient - process stream and avoid buffering whole text
        return new StringReader(apply(CharStreams.toString(reader)));
    }

    @Nullable
    public Writer transform(@Nullable final Writer writer) throws IOException
    {
        if (writer == null) {
            return null;
        }

        // XXX: inefficient - process stream and avoid buffering whole text
        return new StringWriter() {

            @Override
            public void close() throws IOException
            {
                super.close();
                try {
                    writer.write(toString());
                } finally {
                    writer.close();
                }
            }

        };
    }

    @Override
    public boolean equals(final Object object)
    {
        if (object == this) {
            return true;
        }
        if (object == null || !(object instanceof MacroExpander)) {
            return false;
        }
        final MacroExpander other = (MacroExpander) object;
        return this.macroAtoms.keySet().equals(other.macroAtoms.keySet())
                && this.openChar == other.openChar && this.closeChar == other.closeChar
                && this.separatorChar == other.separatorChar;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(this.macroAtoms.keySet().hashCode(), this.openChar,
                this.closeChar, this.separatorChar);
    }

}

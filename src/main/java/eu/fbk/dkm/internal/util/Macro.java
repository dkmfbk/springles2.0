package eu.fbk.dkm.internal.util;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;

public final class Macro
{

    private static final Pattern WORD_PATTERN = Pattern.compile("[a-zA-Z_][a-zA-Z_0-9]*");

    /**
	 * @uml.property  name="name"
	 */
    private final String name;

    /**
	 * @uml.property  name="args"
	 * @uml.associationEnd  multiplicity="(0 -1)" elementType="java.lang.String"
	 */
    private final List<String> args;

    /**
	 * @uml.property  name="template"
	 */
    private final String template;

    public Macro(final String name, final String... argsAndTemplate)
    {
        this(name, Arrays.asList(argsAndTemplate).subList(0, argsAndTemplate.length - 1),
                argsAndTemplate[argsAndTemplate.length - 1]);
    }

    public Macro(final String name, @Nullable final Iterable<String> args, final String template)
    {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(template);

        this.name = name;
        this.args = args == null ? ImmutableList.<String>of() : ImmutableList.copyOf(args);
        this.template = template;
    }

    public boolean isConstant()
    {
        return this.args.isEmpty();
    }

    /**
	 * @return
	 * @uml.property  name="name"
	 */
    public String getName()
    {
        return this.name;
    }

    public List<String> getArgs()
    {
        return this.args;
    }

    /**
	 * @return
	 * @uml.property  name="template"
	 */
    public String getTemplate()
    {
        return this.template;
    }

    @Override
    public boolean equals(final Object object)
    {
        if (object == this) {
            return true;
        }
        if (object == null || !(object instanceof Macro)) {
            return false;
        }
        final Macro other = (Macro) object;
        return this.name.equals(other.name) && this.args.size() == other.args.size();
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(this.name, this.args.size());
    }

    @Override
    public String toString()
    {
        return this.name + (this.args.isEmpty() ? "" : "/" + this.args.size());
    }

    public static List<Macro> read(final String string)
    {
        try {
            return read(new StringReader(string));
        } catch (final IOException ex) {
            throw new Error("Unexpected exception: " + ex.getMessage(), ex);
        }
    }

    public static List<Macro> read(final Readable readable) throws IOException
    {
        final List<String> lines = CharStreams.readLines(readable);
        final List<String> definitions = Lists.newArrayList();
        StringBuilder builder = null;
        for (int i = 0; i < lines.size(); ++i) {
            final String line = lines.get(i).trim();
            if (line.length() > 0) {
                builder = builder != null ? builder : new StringBuilder();
                builder.append(line).append("\n");
            }
            if ((line.length() == 0 || i == lines.size() - 1) && builder.length() > 0) {
                definitions.add(builder.toString());
                builder = null;
            }
        }

        final List<Macro> macros = Lists.newArrayList();
        for (final String definition : definitions) {
            final int index = definition.indexOf('=');
            if (index < 0) {
                throw new Error("Invalid macro definition (syntax error):\n" + definition);
            }
            final String head = definition.substring(0, index).trim();
            final String template = definition.substring(index + 1).trim();
            final int open = head.indexOf('(');
            final int close = head.lastIndexOf(')');
            String name = head;
            final List<String> args = Lists.newArrayList();
            if (open > 0 && close > open) {
                name = head.substring(0, open);
                for (final String arg : Splitter.on(',').trimResults()
                        .split(head.substring(open + 1, close))) {
                    if (!WORD_PATTERN.matcher(arg).matches()) {
                        throw new IllegalArgumentException(
                                "Invalid macro definition (invalid arg name '" + arg + "'):\n"
                                        + definition);
                    }
                    args.add(arg);
                }
            }
            if (!WORD_PATTERN.matcher(name).matches()) {
                throw new IllegalArgumentException(
                        "Invalid macro definition (invalid macro name '" + name + "'):\n"
                                + definition);
            }
            macros.add(new Macro(name, args, template));
        }
        return macros;
    }

    public static void write(final Appendable out, final Iterable<Macro> macros)
            throws IOException
    {
        for (final Macro macro : macros) {
            out.append(macro.name);
            if (!macro.args.isEmpty()) {
                out.append("(").append(Joiner.on(", ").join(macro.args)).append(")");
            }
            out.append(" = ").append(macro.template).append("\n\n");
        }
    }

}
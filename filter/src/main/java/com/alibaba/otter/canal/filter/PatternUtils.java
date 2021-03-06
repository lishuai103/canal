package com.alibaba.otter.canal.filter;

import com.alibaba.otter.canal.filter.exception.CanalFilterException;
import com.google.common.base.Function;
import com.google.common.collect.MapMaker;
import org.apache.oro.text.regex.MalformedPatternException;
import org.apache.oro.text.regex.Pattern;
import org.apache.oro.text.regex.PatternCompiler;
import org.apache.oro.text.regex.Perl5Compiler;

import java.util.Map;

/**
 * 提供{@linkplain Pattern}的lazy get处理
 *
 * @author jianghang 2013-1-22 下午09:36:44
 * @version 1.0.0
 */
public class PatternUtils {

    private static Map<String, Pattern> patterns = new MapMaker().softValues().makeComputingMap(
            new Function<String, Pattern>() {

                public Pattern apply(
                        String pattern) {
                    try {
                        PatternCompiler pc = new Perl5Compiler();
                        return pc.compile(
                                pattern,
                                Perl5Compiler.CASE_INSENSITIVE_MASK
                                        | Perl5Compiler.READ_ONLY_MASK
                                        | Perl5Compiler.SINGLELINE_MASK);
                    } catch (MalformedPatternException e) {
                        throw new CanalFilterException(
                                e);
                    }
                }
            });

    public static Pattern getPattern(String pattern) {
        return patterns.get(pattern);
    }

    public static void clear() {
        patterns.clear();
    }
}

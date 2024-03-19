package com.acco.gmall.realtime.common.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

/**
 * ClassName: IkUtil
 * Description: None
 * Package: com.acco.gmall.realtime.common.util
 *
 * @author : Accoalde
 * @version: 1.0
 * Creat time 2024-03-09 23:02
 */
public class IkUtil {
    public static void main(String[] args) {
        System.out.println(IkUtil.split("lhy mmd 机械革命15pro 拯救者r9000P "));
    }

    public static Set<String> split(String s) {
        Set<String> result = new HashSet<>();
        // String => Reader

        Reader reader = new StringReader(s);
        // 智能分词
        // max_word
        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);

        try {
            Lexeme next = ikSegmenter.next();
            while (next != null) {
                String word = next.getLexemeText();
                result.add(word);
                next = ikSegmenter.next();
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return result;
    }
}

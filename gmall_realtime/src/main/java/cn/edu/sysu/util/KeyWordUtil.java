package cn.edu.sysu.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author : song bei chang
 * @create 2021/11/29 21:42
 */
public class KeyWordUtil {

    public static List<String> analyze(String text) {

        final StringReader reader = new StringReader(text);
        // useSmart=true ，分词器使用智能切分策略， =false则使用细粒度切分
        final IKSegmenter ikSegmenter = new IKSegmenter(reader, true);
        final ArrayList<String> result = new ArrayList<>();
        try {
            Lexeme kw;
            while ((kw = ikSegmenter.next()) != null) {
                String lexemeText = kw.getLexemeText();
                result.add(lexemeText);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;

    }

    public static void main(String[] args) {
        System.out.println(analyze("我是一个中国人"));
        //false [我, 是, 一个中国, 一个, 一, 个中, 个, 中国人, 中国, 国人]
        //true  [我, 是, 一个, 中国人]
    }
}



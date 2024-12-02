package com.atguigu.gmall.realtime.dws.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Felix
 * @date 2024/6/09
 * 分词工具类
 */
public class KeywordUtil {
    //分词
    public static List<String> analyze(String text)//搜索框里搜索的词
    {
        List<String> keywordList = new ArrayList<>();//new一个对象
        StringReader reader = new StringReader(text);
        IKSegmenter ik = new IKSegmenter(reader,true);//转换成输入流对象，true表示打开智能分词模式，自动识别一个词该不该拆开
        try {
            Lexeme lexeme = null;
            while ((lexeme = ik.next())!=null)//有词可以分，
            {
                String keyword = lexeme.getLexemeText();
                keywordList.add(keyword);//分好一个词就放集合里去
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return keywordList;
    }

    public static void main(String[] args)
    {
        System.out.println(analyze("小米手机京东自营5G移动联通电信"));
    }

}

package com.pharbers.StreamEngine.Utils.Annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * 功能描述
 *
 * @author dcs
 * @version 0.0
 * @tparam T 构造泛型参数
 * @note 一些值得注意的地方
 * @since 2019/10/17 10:39
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Component {
    String name();
    String type();
    String factory() default "default";
}

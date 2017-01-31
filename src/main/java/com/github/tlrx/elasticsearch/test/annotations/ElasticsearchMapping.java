/**
 *
 */
package com.github.tlrx.elasticsearch.test.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * ElasticsearchMapping Annotation, used to create mapping for a given document type
 *
 * @author tlrx
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface ElasticsearchMapping {

    /**
     * The type's name for which the mapping is defined
     */
    String typeName();

    /**
     * Fields of the mapping
     */
    ElasticsearchMappingField[] properties() default {};

    /**
     * The source's "enabled" value (default to true)
     */
    boolean source() default true;

    /**
     * The _parent's type
     */
    String parent() default "";

}

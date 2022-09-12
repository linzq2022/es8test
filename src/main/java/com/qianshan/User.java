package com.qianshan;
/**
 * @author qianshan
 * @create 2022-09-12 14:25
 */

import java.io.Serializable;

/**
 * @author qianshan
 * @date 2022年09月12日 14:25
 */
public class User implements Serializable {
    private Integer id;
    private String name;
    private Integer age;

    public User(Integer id, String name, Integer age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    public User() {
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }
}

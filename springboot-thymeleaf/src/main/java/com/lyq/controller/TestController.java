package com.lyq.controller;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * @author 梁耀其
 * 模板引擎
 */
@Controller
@RequestMapping(value = "test")
public class TestController {
    @RequestMapping(value = "/hello")
    public String hello(Model model) {
        model.addAttribute("message","hello thymeleaf");
        return "demo1";
    }
}

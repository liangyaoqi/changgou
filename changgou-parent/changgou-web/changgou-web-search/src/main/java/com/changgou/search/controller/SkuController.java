package com.changgou.search.controller;

import com.changgou.search.feign.SkuFeign;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.Map;

@RequestMapping(value = "/search")
@Controller
public class SkuController {
    
    @Autowired
    private SkuFeign skuFeign;

    @GetMapping(value = "/list")
    public String searchMap(@RequestParam(required = false)Map<String,String> searchMap, Model model){
        Map<String,Object> resultMap = skuFeign.search(searchMap);
        model.addAttribute("result",resultMap);
        return "search";
    }
}

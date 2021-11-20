package com.changgou.search.service;

import java.util.Map;

public interface SkuService {
    /**
     * 导入数据到数据库中
     */
    void importData();

    /**
     * 多条件查询
     * @param searchMap
     * @return
     */
    Map<String,Object> search(Map<String,String> searchMap);
}

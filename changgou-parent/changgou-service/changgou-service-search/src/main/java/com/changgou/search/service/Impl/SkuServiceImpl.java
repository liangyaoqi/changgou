package com.changgou.search.service.Impl;

import com.alibaba.fastjson.JSON;
import com.changgou.entity.Result;
import com.changgou.goods.feign.SkuFeign;
import com.changgou.goods.pojo.Sku;
import com.changgou.search.dao.SkuEsMapper;
import com.changgou.search.pojo.SkuInfo;
import com.changgou.search.service.SkuService;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.hibernate.validator.resourceloading.AggregateResourceBundleLocator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.SearchResultMapper;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.aggregation.impl.AggregatedPageImpl;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class SkuServiceImpl implements SkuService {

    @Autowired
    private SkuFeign skuFeign;

    @Autowired
    private SkuEsMapper skuEsMapper;

    /**
     * 实现对索引库的增删改查
     */
    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;


    /**
     * 旧的分组查询
     * 多条件搜索
     */
    @Override
    public Map<String, Object> search(Map<String,String> searchMap) {
        //搜索条件封装
        NativeSearchQueryBuilder nativeSearchQueryBuilder = buildBasicQuery(searchMap);

        //集合搜索
        Map<String, Object> resultMap = searchList(nativeSearchQueryBuilder);

        //当用户选择了分类，把分类作为搜索条件，则不需要对分类进行分组搜索，以为分组搜索是用于显示分类搜索条件的
        //用户输入的参数->searchMap->category
        //分类分组查询实现
        if (searchMap==null || StringUtils.isEmpty(searchMap.get("category"))) {
            List<String> categoryList = searchCategoryList(nativeSearchQueryBuilder);
            resultMap.put("categoryList", categoryList);
        }

        //当用户选择了品牌，把品牌作为搜索条件，则不需要对品牌进行分组搜索，以为分组搜索是用于显示品牌搜索条件的
        //用户输入的参数->searchMap->brand
        //分类品牌查询实现
        if (searchMap==null || StringUtils.isEmpty(searchMap.get("brand"))) {
            List<String> brandList = searchBrandList(nativeSearchQueryBuilder);
            resultMap.put("brandList", brandList);
        }

        //当用户选择了规格，把规格作为搜索条件，则不需要对规格进行分组搜索，以为分组搜索是用于显示规格搜索条件的
        //分类规格查询实现
        Map<String, Set<String>> specList = searchSpecList(nativeSearchQueryBuilder);
        resultMap.put("specList", specList);

        //新方法实现
        /*Map<String, Object> groupMap = searchGroupList(nativeSearchQueryBuilder, searchMap);
        resultMap.putAll(groupMap);*/

        return resultMap;
    }

    /**
     * 优化后的新分组查询
     * 分类、品牌、规格分组查询(全部分组信息一次查询，减少查询次数优化查询速度）
     * @param nativeSearchQueryBuilder
     * @return
     */
    private Map<String,Object> searchGroupList(NativeSearchQueryBuilder nativeSearchQueryBuilder,Map<String,String> searchMap) {
        /**
         * 分组查询分类集合
         * addAggregation():添加一个聚合查询
         * 根据用户传入的参数进行查询
         */
            nativeSearchQueryBuilder.addAggregation(AggregationBuilders.terms("skuCategory").field("categoryName"));
            nativeSearchQueryBuilder.addAggregation(AggregationBuilders.terms("skuBrand").field("brandName"));
            nativeSearchQueryBuilder.addAggregation(AggregationBuilders.terms("skuSpec").field("spec.keyword"));

        AggregatedPage<SkuInfo> aggregatedPage = elasticsearchTemplate.queryForPage(nativeSearchQueryBuilder.build(), SkuInfo.class);

        /**
         * 获取分页数据
         *aggregatedPage.getAggregations()：获取的是集合，可以根据多个域进行分组
         * get("skuCategory")：指定域的集合 [手机、电视、手机配件]
         */
        //创建一个Map集合，存储所有分组数据
        Map<String, Object> groupMapResult= new HashMap<String,Object>();

        if (searchMap==null || StringUtils.isEmpty(searchMap.get("category"))) {
            StringTerms categoryTerms = aggregatedPage.getAggregations().get("skuCategory");
            List<String> categoryList = getGroupList(categoryTerms);
            groupMapResult.put("categoryList ",categoryList);
        }
        if (searchMap==null || StringUtils.isEmpty(searchMap.get("brand"))) {
            StringTerms brandTerms = aggregatedPage.getAggregations().get("skuBrand");
            List<String> brandList = getGroupList(brandTerms);
            groupMapResult.put("brandList",brandList);
        }
        StringTerms specTerms = aggregatedPage.getAggregations().get("skuSpec");
        List<String> specList = getGroupList(specTerms);
        //规格还需要合并后在存入
        Map<String, Set<String>> specMap = putAllSpec(specList);
        groupMapResult.put("specMap",specMap);

        return groupMapResult;
    }

    /**
     * 获取集合分组数据
     * @param stringTerms
     * @return
     */
    private List<String> getGroupList(StringTerms stringTerms) {
        List<String> groupList = new ArrayList<>();
        for (StringTerms.Bucket bucket : stringTerms.getBuckets()) {
            //其中的一个分类名字
            String fieldName = bucket.getKeyAsString();
            groupList.add(fieldName);
        }
        return groupList;
    }

    /**
     * 规格分组查询
     * @param nativeSearchQueryBuilder
     * @return
     */
    private Map<String, Set<String>> searchSpecList(NativeSearchQueryBuilder nativeSearchQueryBuilder) {
        /**
         * 分组查询规格集合
         * addAggregation():添加一个聚合查询
         */
        nativeSearchQueryBuilder.addAggregation(AggregationBuilders.terms("skuSpec").field("spec.keyword").size(10000));
        AggregatedPage<SkuInfo> aggregatedPage = elasticsearchTemplate.queryForPage(nativeSearchQueryBuilder.build(), SkuInfo.class);

        /**
         * 获取分页数据
         *aggregatedPage.getAggregations()：获取的是集合，可以根据多个域进行分组
         * get("skuSpec")：指定域的集合 {"手机屏幕尺寸":"5寸","网络":"联通2G","颜色":"红","测试":"测试","机身内存":"16G","存储":"16G","像素":"300万像素"}
         */
        StringTerms stringTerms = aggregatedPage.getAggregations().get("skuSpec");

        List<String> specList = new ArrayList<>();
        for (StringTerms.Bucket bucket : stringTerms.getBuckets()) {
            //其中的一个规格名字
            String specName = bucket.getKeyAsString();
            specList.add(specName);
        }

        Map<String, Set<String>> allSpec = putAllSpec(specList);

        return allSpec;
    }

    /**
     * 规格汇总合并
     * @param specList
     * @return
     */
    private Map<String, Set<String>> putAllSpec(List<String> specList) {
        Map<String, Set<String>> allSpec = new HashMap<>();
        //循环遍历specList
        for (String spec : specList) {
            //将specList中的每一个JSON字符串转成Map
            Map<String,String> specMap = JSON.parseObject(spec, Map.class);
            //将每一个Map对象合成成一个Map<String,Set<String>>
            //循环Map
            for (Map.Entry<String, String> entry : specMap.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();

                //将数据合并到set集合中，从allSpec中获取当前规格对应的Set(get()->可以获得Set集合)，直接创建的话value不会存在一个set中
                Set<String> specSet = allSpec.get(key);
                //如果规格还未生成，就先生成再放入值
                if (specSet == null){
                     specSet = new HashSet<>();
                }
                specSet.add(value);
                //完成后再把Set集合放入Map中
                allSpec.put(key,specSet);
            }
        }
        return allSpec;
    }

    /**
     * 品牌分组查询
     * @param nativeSearchQueryBuilder
     * @return
     */
    private List<String> searchBrandList(NativeSearchQueryBuilder nativeSearchQueryBuilder) {
        /**
         * 分组查询品牌集合
         * addAggregation():添加一个聚合查询
         */
        nativeSearchQueryBuilder.addAggregation(AggregationBuilders.terms("skuBrand").field("brandName"));
        AggregatedPage<SkuInfo> aggregatedPage = elasticsearchTemplate.queryForPage(nativeSearchQueryBuilder.build(), SkuInfo.class);

        /**
         * 获取分页数据
         *aggregatedPage.getAggregations()：获取的是集合，可以根据多个域进行分组
         * get("skuBrand")：指定域的集合 [手机、电视、手机配件]
         */
        StringTerms stringTerms = aggregatedPage.getAggregations().get("skuBrand");

        List<String> brandList = new ArrayList<>();
        for (StringTerms.Bucket bucket : stringTerms.getBuckets()) {
            //其中的一个品牌名字
            String categoryName = bucket.getKeyAsString();
            brandList.add(categoryName);
        }
        return brandList;
    }

    /**
     * 搜索条件封装
     * @param searchMap
     * @return
     */
    private NativeSearchQueryBuilder buildBasicQuery(Map<String, String> searchMap) {
        //构建搜索条件对象，用于封装搜索条件
        NativeSearchQueryBuilder nativeSearchQueryBuilder = new NativeSearchQueryBuilder();

        //BoolQuery  must,must_not,shou
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        //构建条件
        if(searchMap !=null && searchMap.size() > 0){
            String keywords = searchMap.get("keywords");
            //关键词不为空，则搜索关键词数据
            if (!StringUtils.isEmpty(keywords)){
                //nativeSearchQueryBuilder.withQuery(QueryBuilders.queryStringQuery(keywords).field("name"));
                boolQueryBuilder.must(QueryBuilders.queryStringQuery(keywords).field("name"));
            }
            //输入了分类->category
            if (!StringUtils.isEmpty(searchMap.get("category"))){
                boolQueryBuilder.must(QueryBuilders.termQuery("brandName",searchMap.get("category")));
            }
            //输入了品牌->brand
            if (!StringUtils.isEmpty(searchMap.get("brand"))){
                boolQueryBuilder.must(QueryBuilders.termQuery("brandName",searchMap.get("brand")));
            }
            //规格过滤,点击选择规格时，如给规格带上”spec_内存“
            for (Map.Entry<String, String> entry : searchMap.entrySet()) {
                String key = entry.getKey();
                if (key.startsWith("spec_")){
                    String value = entry.getValue();
                    //通过截取获得filter名
                    boolQueryBuilder.must(QueryBuilders.termQuery("specMap"+key.substring(5)+".keyword",value));

                }
            }
            //price 0-500元 ... 3000元以上
            String price = searchMap.get("price");
            if (!StringUtils.isEmpty(price)) {
                //去除格式（中文-元): 0-500
                price=price.replaceAll("元", "").replaceAll("以上", "");
                //price[]根据-分割 [0,500]
                String[] prices = price.split("-");
                //边界值匹配
                if (prices!=null && prices.length > 0){
                    boolQueryBuilder.must(QueryBuilders.rangeQuery("price").gt(Integer.parseInt(prices[0])));
                }
                if (prices.length==2){
                    boolQueryBuilder.must(QueryBuilders.rangeQuery("price").lte(Integer.parseInt(prices[1])));
                }
            }
            //prices[0]!=null price>prices[0]
        }

        //分页，如果用户不传分页参数。默认第一页
        Integer pageNum = coverPage(searchMap);
        Integer pageSize=30;
        nativeSearchQueryBuilder.withPageable(PageRequest.of(pageNum-1,pageSize));

        //排序，用户传入要进行排序的域和规则（升序或者降序）
        String sortFiled = searchMap.get("sortFiled");
        String sortRule = searchMap.get("sortRule");
        if (!StringUtils.isEmpty(sortFiled)  && !StringUtils.isEmpty(sortRule)) {
            //指定搜索域
            nativeSearchQueryBuilder.withSort(new FieldSortBuilder(sortFiled)
                    //指定搜索规则
                    .order(SortOrder.valueOf(sortRule)));
        }

        //将boolQueryBuilder给nativeSearchQueryBuilder返回
        nativeSearchQueryBuilder.withQuery(boolQueryBuilder);

        return nativeSearchQueryBuilder;
    }

    /**
     * 接收前端传入的分页参数
     */
    public Integer coverPage(Map<String,String> searchMap){
        if (searchMap != null) {
            String pageNum = searchMap.get("pageNum");
            try {
                return Integer.parseInt(pageNum);
            } catch (NumberFormatException e) {

            }
        }
        return 1;
    }

    /**
     * 数据结果集搜索
     * @param nativeSearchQueryBuilder
     * @return
     */
    private Map<String, Object> searchList(NativeSearchQueryBuilder nativeSearchQueryBuilder) {
        //高亮配置
        HighlightBuilder.Field field = new HighlightBuilder.Field("name");
        //前缀
        field.preTags("<em style=\"color:red;\">");
        //后缀
        field.postTags("</em>");
        //碎片长度（高亮数据长度）
        field.fragmentSize(100);
        //添加高亮
        nativeSearchQueryBuilder.withHighlightFields(field);

        /**
         *执行搜索，返回执行结果
         * 1:搜索条件对象
         * 2：结果集需要转换的类型
         */
        AggregatedPage<SkuInfo> page = elasticsearchTemplate
                /*搜索条件封装*/
                .queryForPage(nativeSearchQueryBuilder.build(),
                        /*数据集合要转换的类型*/
                        SkuInfo.class,
                        /*执行搜索后，将数据结果集封装到该象中*/
                        new SearchResultMapper() {
                            @Override
                            public <T> AggregatedPage<T> mapResults(SearchResponse searchResponse, Class<T> aClass, Pageable pageable) {
                                //执行查询，获取所有数据，结果集[非高亮数据|高亮数据]
                                List<T> list= new ArrayList<>();
                                for (SearchHit hit : searchResponse.getHits()) {
                                    //分析结果集数据，获取非高亮数据
                                    SkuInfo skuInfo = JSON.parseObject(hit.getSourceAsString(), SkuInfo.class);
                                    //分析结果集数据，获取高亮数据->只有某个域的高亮数据
                                    HighlightField highlightField = hit.getHighlightFields().get("name");

                                    if (highlightField != null && highlightField.getFragments()!=null) {
                                        //将高亮数据读取出来
                                        Text[] fragments = highlightField.getFragments();
                                        StringBuffer buffer = new StringBuffer();
                                        for (Text fragment : fragments) {
                                           buffer.append(fragment.toString());
                                        }
                                        //非高亮数据中指定的域替换成高亮数据
                                        skuInfo.setName(buffer.toString());
                                    }
                                    //将高亮数据加入集合中
                                    list.add((T) skuInfo);
                                }
                                /**
                                 * 将数据返回
                                 * 参数：
                                 *  搜索的集合数据，携带高亮的List<T> content
                                 *  分页对象信息
                                 *  搜索记录的总条数
                                 */
                                return new AggregatedPageImpl<T>(list,pageable,searchResponse.getHits().getTotalHits());
                            }
                        });


        //分页参数-总记录数
        long totalElements = page.getTotalElements();
        //总页数
        int totalPages = page.getTotalPages();
        //数据结果集
        List<SkuInfo> contents = page.getContent();

        //封装一个Map对象存储结果
        Map<String, Object> resultMap = new HashMap<String,Object>();
        resultMap.put("rows",contents);
        resultMap.put("total",totalElements);
        resultMap.put("totalPages",totalPages);
        return resultMap;
    }

    /**
     * 分类分组查询
     * @param nativeSearchQueryBuilder
     * @return
     */
    private List<String> searchCategoryList(NativeSearchQueryBuilder nativeSearchQueryBuilder) {
        /**
         * 分组查询分类集合
         * addAggregation():添加一个聚合查询
         */
        nativeSearchQueryBuilder.addAggregation(AggregationBuilders.terms("skuCategory").field("categoryName"));
        AggregatedPage<SkuInfo> aggregatedPage = elasticsearchTemplate.queryForPage(nativeSearchQueryBuilder.build(), SkuInfo.class);

        /**
         * 获取分页数据
         *aggregatedPage.getAggregations()：获取的是集合，可以根据多个域进行分组
         * get("skuCategory")：指定域的集合 [手机、电视、手机配件]
         */
        StringTerms stringTerms = aggregatedPage.getAggregations().get("skuCategory");

        List<String> categoryList = new ArrayList<>();
        for (StringTerms.Bucket bucket : stringTerms.getBuckets()) {
            //其中的一个分类名字
            String categoryName = bucket.getKeyAsString();
            categoryList.add(categoryName);
        }
        return categoryList;
    }

    /**
         * 导入索引库
         */
        @Override
        public void importData() {
            //调用feign
            Result<List<Sku>> skuResult = skuFeign.findAll();
            //将List<Sku>转成<SkuInfo>
            List<SkuInfo> skuInfoList = JSON.parseArray(JSON.toJSONString(skuResult.getData()),SkuInfo.class);
            //循环SkuInfoList
            for (SkuInfo skuInfo : skuInfoList) {
                //{"电视音响效果":"立体声","电视屏幕尺寸":"20英寸","尺码":"165"}
                Map<String,Object> specMap = JSON.parseObject(skuInfo.getSpec(), Map.class);
                //生成动态的域->将该域存入到一个Map<String,Object>对象中即可，该Map<String,Object>会生成一个域，域的名字为Map的key
                skuInfo.setSpecMap(specMap);
            }
            //调用Dao实现数据批量导入
            skuEsMapper.saveAll(skuInfoList);
        }

}

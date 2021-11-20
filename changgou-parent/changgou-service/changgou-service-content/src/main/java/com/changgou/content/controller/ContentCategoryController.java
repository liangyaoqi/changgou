package com.changgou.content.controller;

import com.changgou.content.pojo.ContentCategory;
import com.changgou.content.service.ContentCategoryService;
import com.changgou.entity.Result;
import com.changgou.entity.StatusCode;
import com.github.pagehelper.PageInfo;
import io.swagger.annotations.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/****
 * @Author:
 * @Description:
 * @Date 2019/6/14 0:18
 *****/
@Api(value = "ContentCategoryController")
@RestController
@RequestMapping("/contentCategory")
@CrossOrigin
public class ContentCategoryController {

    @Autowired
    private ContentCategoryService contentCategoryService;

    /***
     * ContentCategory分页条件搜索实现
     * @param contentCategory
     * @param page
     * @param size
     * @return
     */
    @ApiOperation(value = "ContentCategory条件分页查询",notes = "分页条件查询ContentCategory方法详情",tags = {"ContentCategoryController"})
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "path", name = "page", value = "当前页", required = true, dataType = "Integer"),
            @ApiImplicitParam(paramType = "path", name = "size", value = "每页显示条数", required = true, dataType = "Integer")
    })
    @PostMapping(value = "/search/{page}/{size}" )
    public Result<PageInfo> findPage(@RequestBody(required = false) @ApiParam(name = "ContentCategory对象",value = "传入JSON数据",required = false) ContentCategory contentCategory, @PathVariable  int page, @PathVariable  int size){
        //调用ContentCategoryService实现分页条件查询ContentCategory
        PageInfo<ContentCategory> pageInfo = contentCategoryService.findPage(contentCategory, page, size);
        return new Result(true, StatusCode.OK,"查询成功",pageInfo);
    }

    /***
     * ContentCategory分页搜索实现
     * @param page:当前页
     * @param size:每页显示多少条
     * @return
     */
    @ApiOperation(value = "ContentCategory分页查询",notes = "分页查询ContentCategory方法详情",tags = {"ContentCategoryController"})
    @ApiImplicitParams({
            @ApiImplicitParam(paramType = "path", name = "page", value = "当前页", required = true, dataType = "Integer"),
            @ApiImplicitParam(paramType = "path", name = "size", value = "每页显示条数", required = true, dataType = "Integer")
    })
    @GetMapping(value = "/search/{page}/{size}" )
    public Result<PageInfo> findPage(@PathVariable  int page, @PathVariable  int size){
        //调用ContentCategoryService实现分页查询ContentCategory
        PageInfo<ContentCategory> pageInfo = contentCategoryService.findPage(page, size);
        return new Result<PageInfo>(true,StatusCode.OK,"查询成功",pageInfo);
    }

    /***
     * 多条件搜索品牌数据
     * @param contentCategory
     * @return
     */
    @ApiOperation(value = "ContentCategory条件查询",notes = "条件查询ContentCategory方法详情",tags = {"ContentCategoryController"})
    @PostMapping(value = "/search" )
    public Result<List<ContentCategory>> findList(@RequestBody(required = false) @ApiParam(name = "ContentCategory对象",value = "传入JSON数据",required = false) ContentCategory contentCategory){
        //调用ContentCategoryService实现条件查询ContentCategory
        List<ContentCategory> list = contentCategoryService.findList(contentCategory);
        return new Result<List<ContentCategory>>(true,StatusCode.OK,"查询成功",list);
    }

    /***
     * 根据ID删除品牌数据
     * @param id
     * @return
     */
    @ApiOperation(value = "ContentCategory根据ID删除",notes = "根据ID删除ContentCategory方法详情",tags = {"ContentCategoryController"})
    @ApiImplicitParam(paramType = "path", name = "id", value = "主键ID", required = true, dataType = "Long")
    @DeleteMapping(value = "/{id}" )
    public Result delete(@PathVariable Long id){
        //调用ContentCategoryService实现根据主键删除
        contentCategoryService.delete(id);
        return new Result(true,StatusCode.OK,"删除成功");
    }

    /***
     * 修改ContentCategory数据
     * @param contentCategory
     * @param id
     * @return
     */
    @ApiOperation(value = "ContentCategory根据ID修改",notes = "根据ID修改ContentCategory方法详情",tags = {"ContentCategoryController"})
    @ApiImplicitParam(paramType = "path", name = "id", value = "主键ID", required = true, dataType = "Long")
    @PutMapping(value="/{id}")
    public Result update(@RequestBody @ApiParam(name = "ContentCategory对象",value = "传入JSON数据",required = false) ContentCategory contentCategory,@PathVariable Long id){
        //设置主键值
        contentCategory.setId(id);
        //调用ContentCategoryService实现修改ContentCategory
        contentCategoryService.update(contentCategory);
        return new Result(true,StatusCode.OK,"修改成功");
    }

    /***
     * 新增ContentCategory数据
     * @param contentCategory
     * @return
     */
    @ApiOperation(value = "ContentCategory添加",notes = "添加ContentCategory方法详情",tags = {"ContentCategoryController"})
    @PostMapping
    public Result add(@RequestBody  @ApiParam(name = "ContentCategory对象",value = "传入JSON数据",required = true) ContentCategory contentCategory){
        //调用ContentCategoryService实现添加ContentCategory
        contentCategoryService.add(contentCategory);
        return new Result(true,StatusCode.OK,"添加成功");
    }

    /***
     * 根据ID查询ContentCategory数据
     * @param id
     * @return
     */
    @ApiOperation(value = "ContentCategory根据ID查询",notes = "根据ID查询ContentCategory方法详情",tags = {"ContentCategoryController"})
    @ApiImplicitParam(paramType = "path", name = "id", value = "主键ID", required = true, dataType = "Long")
    @GetMapping("/{id}")
    public Result<ContentCategory> findById(@PathVariable Long id){
        //调用ContentCategoryService实现根据主键查询ContentCategory
        ContentCategory contentCategory = contentCategoryService.findById(id);
        return new Result<ContentCategory>(true,StatusCode.OK,"查询成功",contentCategory);
    }

    /***
     * 查询ContentCategory全部数据
     * @return
     */
    @ApiOperation(value = "查询所有ContentCategory",notes = "查询所ContentCategory有方法详情",tags = {"ContentCategoryController"})
    @GetMapping
    public Result<List<ContentCategory>> findAll(){
        //调用ContentCategoryService实现查询所有ContentCategory
        List<ContentCategory> list = contentCategoryService.findAll();
        return new Result<List<ContentCategory>>(true, StatusCode.OK,"查询成功",list) ;
    }
}

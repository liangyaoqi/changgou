package com.changgou.controller;

import com.changgou.file.FastDFSFile;
import com.changgou.util.FastDFSUtil;
import com.changgou.entity.Result;
import com.changgou.entity.StatusCode;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

@RestController
@CrossOrigin
@RequestMapping(value = "/upload")
public class FileUploadController {



    @PostMapping
    public Result upload(@RequestParam(value = "file")MultipartFile file)throws  Exception{
        //封装文件信息
        FastDFSFile fastDFSFile = new FastDFSFile(
                //文件名字
                file.getOriginalFilename(),
                //文件字节数组
                file.getBytes(),
                //文件拓展名
                StringUtils.getFilenameExtension(file.getOriginalFilename())
        );

        //调用FastDFSUtil工具类将文件传入FastDFS中
        String[] uploads = FastDFSUtil.upload(fastDFSFile);
        //拼接文件的url地址
        //String url ="http://192.168.126.129:8080/"+uploads[0]+"/"+uploads[1];
        String url = FastDFSUtil.getTrackerInfo()+"/"+uploads[0]+"/"+uploads[1];
        return new Result(true, StatusCode.OK,"文件上传成功",url);
    }

}

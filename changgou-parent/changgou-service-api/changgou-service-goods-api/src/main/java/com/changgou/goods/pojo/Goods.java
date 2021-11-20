package com.changgou.goods.pojo;

import java.io.Serializable;
import java.util.List;

/**
 * 商品信息组合对象
 *Spu
 * List<Spk>
 */
 public class Goods implements Serializable {
    private Spu spu;
    private List<Sku> skuList;

    public Spu getSpu() {
        return spu;
    }

    public void setSpu(Spu spu) {
        this.spu = spu;
    }

    public List<Sku> getSkuList() {
        return skuList;
    }

    public void setSkuList(List<Sku> skuList) {
        this.skuList = skuList;
    }
}

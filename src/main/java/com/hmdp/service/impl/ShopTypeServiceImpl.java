package com.hmdp.service.impl;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.util.List;

import static com.hmdp.utils.RedisConstants.CACHE_SHOPTYPE_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result typeQuery() {
        String shopTypeJson = stringRedisTemplate.opsForValue().get(CACHE_SHOPTYPE_KEY);
        //存在redis
        if(StrUtil.isNotBlank(shopTypeJson)){
            List<ShopType> shopTypes = JSONUtil.toList(shopTypeJson, ShopType.class);
            return Result.ok(shopTypes);
        }
        //不存在查数据库
        List<ShopType> list = query().orderByAsc("sort").list();
        if(CollectionUtil.isEmpty(list)){
            return Result.ok(list);
        }
        stringRedisTemplate.opsForValue().set(CACHE_SHOPTYPE_KEY,JSONUtil.toJsonStr(list));
        return Result.ok(list);
    }
}

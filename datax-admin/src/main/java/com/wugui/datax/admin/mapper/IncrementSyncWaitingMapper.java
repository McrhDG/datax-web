package com.wugui.datax.admin.mapper;

import com.wugui.datax.admin.entity.IncrementSyncWaiting;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;


/**
 * 增量同步等待表
 *
 * @author chen.ruihong
 * @date 2020-06-25
 */
@Mapper
public interface IncrementSyncWaitingMapper {

    List<IncrementSyncWaiting> findAll();

    int save(IncrementSyncWaiting info);

    IncrementSyncWaiting loadById(@Param("id") int id);

    int update(IncrementSyncWaiting jobInfo);

    int delete(@Param("id") long id);

    List<IncrementSyncWaiting> loadByJobId(@Param("jobId") int jobId);

    int deleteByJobId(@Param("jobId") int jobId);

    int deleteByJobIdAndIdValue(@Param("jobId") int jobId, @Param("idValue") String idValue);

    IncrementSyncWaiting loadUpdate(@Param("jobId") int jobId);

    int deleteByOperation(@Param("jobId") int jobId, @Param("idValue") String idValue, @Param("operationType") String operationType);

    int updateContent(@Param("id") int id, @Param("content") String content);

    List<Integer> findJobIds();
}

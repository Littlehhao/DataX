package com.alibaba.datax.core.transport.transformer;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.util.AesUtil;
import com.alibaba.datax.transformer.Transformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * @program: datax-all
 * @description: 解密
 * @author: little_hao
 * @create: 2021-11-12 17:34
 **/
public class AESDecryptTransformer extends Transformer {

    private static final Logger LOG = LoggerFactory.getLogger(AESDecryptTransformer.class);

    int columnIndex;

    public AESDecryptTransformer() {
        setTransformerName("dx_decrypt_aes");
        LOG.info("Using AES preserve masker");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {
        try {
            if (paras.length < 1) {
                throw new RuntimeException("dx_aes transformer缺少参数");
            }
            columnIndex = (Integer) paras[0];
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }
        Column column = record.getColumn(columnIndex);
        try {
            String oriValue = column.asString();
            if (oriValue == null) {
                return record;
            }
            if(column.getType() == Column.Type.STRING) {
                String decrypt = AesUtil.decrypt(oriValue);
                record.setColumn(columnIndex, new StringColumn(decrypt));
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
        }
        return record;
    }
}

package com.alibaba.datax.core.transport.transformer;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.transformer.ComplexTransformer;
import org.bson.types.ObjectId;

import java.util.Arrays;
import java.util.Map;

/**常量替换
 * 单纯进行字符量的替换
 * TODO 后续考虑类型转化，转化动作考虑在参数生成的时候执行，以在一定程度上优化性能，不过代码侵入较多
 * 参数：修改的记录位置，替换的内容，是否需要传入常量的类型，不在这转，那就是交给写入端按类型转化
 * */
public class ConstantReplaceTransformer extends ComplexTransformer {

    public ConstantReplaceTransformer() {
        setTransformerName("dx_constant_replace");
    }

    @Override
    public Record evaluate(Record record, Map<String, Object> tContext, Object... paras) {

        int columnIndex;
        String value;

        // 根据值构建Column类型
        try {
            if (paras.length != 2) {
                throw new RuntimeException(getTransformerName()+" paras must be 2");
            }
            columnIndex = (Integer) paras[0];
            value = (String) paras[1];
            if(value.startsWith("$")){
                if("${$id}".equals(value)){
                    value = ObjectId.get().toString();
                }
            }
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }

        try {
            // setColumn中会自动对长度不足的地方填充 null值
            record.setColumn(columnIndex, new StringColumn(value));
        } catch (Exception e) {
            throw DataXException.asDataXException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(),e);
        }
        return record;
    }
}

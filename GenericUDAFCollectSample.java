/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//package com.conviva.d3.udf;
import java.util.ArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * GenericUDAFCollectSet
 */
@Description(name = "collect_sample", value = "_FUNC_(x, sample_threshold) - Returns a list of objects")
public class GenericUDAFCollectSample extends AbstractGenericUDAFResolver {

    public GenericUDAFCollectSample() {
    }

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {

        if (parameters.length < 2) {
            throw new UDFArgumentTypeException(parameters.length - 1, "Atleast two arguments are expected.");
        }

        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "Only primitive type arguments are accepted but " + parameters[0].getTypeName() + " was passed as parameter 1.");
        }

        if (parameters[1].getCategory() != ObjectInspector.Category.PRIMITIVE)
        {
            throw new UDFArgumentTypeException(1, "Only primitive double types are accepted by collect_sample.");
        }

        if (((PrimitiveTypeInfo) parameters[1]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.DOUBLE)
        {
            throw new UDFArgumentTypeException(1, "Only primitive double types are accepted by collect_sample");
        }

        return new GenericUDAFMkSampleEvaluator();
    }

    public static class GenericUDAFMkSampleEvaluator extends GenericUDAFEvaluator {

        // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
        private PrimitiveObjectInspector inputOI;
        private PrimitiveObjectInspector sampThreshOI;
        private PrimitiveObjectInspector listSizeOI;

        // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list
        // of objs)
        private transient StandardListObjectInspector loi;

        private transient StandardListObjectInspector internalMergeOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {

            super.init(m, parameters);
            // init output object inspectors
            // The output of a partial aggregation is a list
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
                sampThreshOI = (PrimitiveObjectInspector) parameters[1];
                if(parameters.length == 3)
                    listSizeOI = (PrimitiveObjectInspector) parameters[2];

                return ObjectInspectorFactory.getStandardListObjectInspector((PrimitiveObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(inputOI));
            } else {
                if (!(parameters[0] instanceof StandardListObjectInspector)) {
                    //no map aggregation.
                    inputOI = (PrimitiveObjectInspector)  ObjectInspectorUtils.getStandardObjectInspector(parameters[0]);
                    return (StandardListObjectInspector) ObjectInspectorFactory.getStandardListObjectInspector(inputOI);
                } else {
                    internalMergeOI = (StandardListObjectInspector) parameters[0];
                    inputOI = (PrimitiveObjectInspector) internalMergeOI.getListElementObjectInspector();
                    loi = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);
                    return loi;
                }
            }
        }

        static class MkArrayAggregationBuffer implements AggregationBuffer {
            ArrayList<Object> container;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((MkArrayAggregationBuffer) agg).container = new ArrayList<Object>();
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            MkArrayAggregationBuffer ret = new MkArrayAggregationBuffer();
            reset(ret);
            return ret;
        }

        //mapside
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length >= 2);

            if (parameters[0] == null || parameters[1] == null){
                return;
            }
            Object p = parameters[0];
            double sampThresh = PrimitiveObjectInspectorUtils.getDouble(parameters[1], sampThreshOI);
            if (sampThresh > 1 || sampThresh < 0){
                throw new HiveException(getClass().getSimpleName() + "Sample Threshold must be between 0 and 1 inclusive (0,1)");
            }
            double rnd = Math.random();

            if (rnd >= 0 && rnd <= sampThresh) {
                MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
                putIntoList(p, myagg);
            }
        }

        //mapside
        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
            ArrayList<Object> ret = new ArrayList<Object>(myagg.container.size());
            ret.addAll(myagg.container);
            return ret;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
                ArrayList<Object> partialResult = (ArrayList<Object>) internalMergeOI.getList(partial);
                for (Object i : partialResult) {
                    putIntoList(i, myagg);
                }
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            MkArrayAggregationBuffer myagg = (MkArrayAggregationBuffer) agg;
            ArrayList<Object> ret = new ArrayList<Object>(myagg.container.size());
            ret.addAll(myagg.container);
            return ret;
        }

        private void putIntoList(Object p, MkArrayAggregationBuffer myagg) {
            Object pCopy = ObjectInspectorUtils.copyToStandardObject(p, this.inputOI);
            myagg.container.add(pCopy);
        }
    }

}

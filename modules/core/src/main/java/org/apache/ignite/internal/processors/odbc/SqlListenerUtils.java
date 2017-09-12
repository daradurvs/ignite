/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.odbc;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.UUID;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.jetbrains.annotations.Nullable;

/**
 * Binary reader with marshaling non-primitive and non-embedded objects with JDK marshaller.
 */
@SuppressWarnings("unchecked")
public abstract class SqlListenerUtils {
    /**
     * @param reader Reader.
     * @param binObjAllow Allow to read non plaint objects.
     * @return Read object.
     * @throws BinaryObjectException On error.
     */
    @Nullable public static Object readObject(BinaryReaderExImpl reader, boolean binObjAllow)
        throws BinaryObjectException {
        byte type = reader.readByte();

        switch (type) {
            case GridBinaryMarshaller.NULL:
                return null;

            case GridBinaryMarshaller.BOOLEAN:
                return reader.readBoolean();

            case GridBinaryMarshaller.BYTE:
                return reader.readByte();

            case GridBinaryMarshaller.CHAR:
                return reader.readChar();

            case GridBinaryMarshaller.SHORT:
                return reader.readShort();

            case GridBinaryMarshaller.INT:
                return reader.readInt();

            case GridBinaryMarshaller.LONG:
                return reader.readLong();

            case GridBinaryMarshaller.FLOAT:
                return reader.readFloat();

            case GridBinaryMarshaller.DOUBLE:
                return reader.readDouble();

            case GridBinaryMarshaller.STRING:
                return reader.readString();

            case GridBinaryMarshaller.DECIMAL:
                return reader.readDecimal();

            case GridBinaryMarshaller.UUID:
                return BinaryUtils.doReadUuid(reader.in());

            case GridBinaryMarshaller.TIME:
                return BinaryUtils.doReadTime(reader.in());

            case GridBinaryMarshaller.TIMESTAMP:
                return BinaryUtils.doReadTimestamp(reader.in());

            case GridBinaryMarshaller.DATE:
                return BinaryUtils.doReadDate(reader.in());

            case GridBinaryMarshaller.BOOLEAN_ARR:
                return reader.readBooleanArray();

            case GridBinaryMarshaller.BYTE_ARR:
                return reader.readByteArray();

            case GridBinaryMarshaller.CHAR_ARR:
                return reader.readCharArray();

            case GridBinaryMarshaller.SHORT_ARR:
                return reader.readShortArray();

            case GridBinaryMarshaller.INT_ARR:
                return reader.readIntArray();

            case GridBinaryMarshaller.LONG_ARR:
                return reader.readLongArray();

            case GridBinaryMarshaller.FLOAT_ARR:
                return reader.readFloatArray();

            case GridBinaryMarshaller.DOUBLE_ARR:
                return reader.readDoubleArray();

            case GridBinaryMarshaller.STRING_ARR:
                return reader.readStringArray();

            case GridBinaryMarshaller.DECIMAL_ARR:
                return reader.readDecimalArray();

            case GridBinaryMarshaller.UUID_ARR:
                return reader.readUuidArray();

            case GridBinaryMarshaller.TIME_ARR:
                return reader.readTimeArray();

            case GridBinaryMarshaller.TIMESTAMP_ARR:
                return reader.readTimestampArray();

            case GridBinaryMarshaller.DATE_ARR:
                return reader.readDateArray();

            default:
                reader.in().position(reader.in().position() - 1);

                if (binObjAllow)
                    return reader.readObjectDetached();
                else
                    throw new BinaryObjectException("Custom objects are not supported");
        }
    }

    /**
     * @param writer Writer.
     * @param obj Object to write.
     * @param binObjAllow Allow to write non plain objects.
     * @throws BinaryObjectException On error.
     */
    public static void writeObject(BinaryWriterExImpl writer, @Nullable Object obj, boolean binObjAllow)
        throws BinaryObjectException {
        if (obj == null) {
            writer.writeByte(GridBinaryMarshaller.NULL);

            return;
        }

        Class<?> cls = obj.getClass();

        if (cls == Boolean.class)
            writer.writeBooleanFieldPrimitive((Boolean)obj);
        else if (cls == Byte.class)
            writer.writeByteFieldPrimitive((Byte)obj);
        else if (cls == Character.class)
            writer.writeCharFieldPrimitive((Character)obj);
        else if (cls == Short.class)
            writer.writeShortFieldPrimitive((Short)obj);
        else if (cls == Integer.class)
            writer.writeIntFieldPrimitive((Integer)obj);
        else if (cls == Long.class)
            writer.writeLongFieldPrimitive((Long)obj);
        else if (cls == Float.class)
            writer.writeFloatFieldPrimitive((Float)obj);
        else if (cls == Double.class)
            writer.writeDoubleFieldPrimitive((Double)obj);
        else if (cls == String.class)
            writer.doWriteString((String)obj);
        else if (cls == BigDecimal.class)
            writer.doWriteDecimal((BigDecimal)obj);
        else if (cls == UUID.class)
            writer.writeUuid((UUID)obj);
        else if (cls == Time.class)
            writer.writeTime((Time)obj);
        else if (cls == Timestamp.class)
            writer.writeTimestamp((Timestamp)obj);
        else if (cls == java.sql.Date.class || cls == java.util.Date.class)
            writer.writeDate((java.util.Date)obj);
        else if (cls == boolean[].class)
            writer.writeBooleanArray((boolean[])obj);
        else if (cls == byte[].class)
            writer.writeByteArray((byte[])obj);
        else if (cls == char[].class)
            writer.writeCharArray((char[])obj);
        else if (cls == short[].class)
            writer.writeShortArray((short[])obj);
        else if (cls == int[].class)
            writer.writeIntArray((int[])obj);
        else if (cls == long[].class)
            writer.writeLongArray((long[])obj);
        else if (cls == float[].class)
            writer.writeFloatArray((float[])obj);
        else if (cls == double[].class)
            writer.writeDoubleArray((double[])obj);
        else if (cls == String[].class)
            writer.writeStringArray((String[])obj);
        else if (cls == BigDecimal[].class)
            writer.writeDecimalArray((BigDecimal[])obj);
        else if (cls == UUID[].class)
            writer.writeUuidArray((UUID[])obj);
        else if (cls == Time[].class)
            writer.writeTimeArray((Time[])obj);
        else if (cls == Timestamp[].class)
            writer.writeTimestampArray((Timestamp[])obj);
        else if (cls == java.util.Date[].class || cls == java.sql.Date[].class)
            writer.writeDateArray((java.util.Date[])obj);
        else if (binObjAllow)
            writer.writeObjectDetached(obj);
        else
            throw new BinaryObjectException("Custom objects are not supported");
    }

    /**
     * @param cls Class.
     * @return {@code true} is the type is plain (not user's custom class).
     */
    public static boolean isPlainType(Class<?> cls) {
        return cls == Boolean.class
            || cls == Byte.class
            || cls == Character.class
            || cls == Short.class
            || cls == Integer.class
            || cls == Long.class
            || cls == Float.class
            || cls == Double.class
            || cls == String.class
            || cls == BigDecimal.class
            || cls == UUID.class
            || cls == Time.class
            || cls == Timestamp.class
            || cls == java.sql.Date.class || cls == java.util.Date.class
            || cls == boolean[].class
            || cls == byte[].class
            || cls == char[].class
            || cls == short[].class
            || cls == int[].class
            || cls == long[].class
            || cls == float[].class
            || cls == double[].class
            || cls == String[].class
            || cls == BigDecimal[].class
            || cls == UUID[].class
            || cls == Time[].class
            || cls == Timestamp[].class
            || cls == java.util.Date[].class || cls == java.sql.Date[].class;
    }
}

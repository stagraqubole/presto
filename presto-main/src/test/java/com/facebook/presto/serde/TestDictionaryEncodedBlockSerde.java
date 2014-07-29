/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.serde;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockEncoding;
import io.airlift.slice.DynamicSliceOutput;
import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockAssertions.assertBlockEquals;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TestDictionaryEncodedBlockSerde
{
    @Test
    public void testRoundTrip()
    {
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus());
        VARCHAR.writeString(blockBuilder, "alice");
        VARCHAR.writeString(blockBuilder, "bob");
        VARCHAR.writeString(blockBuilder, "charlie");
        VARCHAR.writeString(blockBuilder, "dave");
        Block block = blockBuilder.build();

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1024);
        BlockEncoding blockEncoding = new DictionaryEncoder(VARCHAR, new UncompressedEncoder(VARCHAR, sliceOutput)).append(block).append(block).append(block).finish();
        Block actualBlock = blockEncoding.readBlock(sliceOutput.slice().getInput());

        BlockBuilder expectedBlockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus());
        VARCHAR.writeString(expectedBlockBuilder, "alice");
        VARCHAR.writeString(expectedBlockBuilder, "bob");
        VARCHAR.writeString(expectedBlockBuilder, "charlie");
        VARCHAR.writeString(expectedBlockBuilder, "dave");
        VARCHAR.writeString(expectedBlockBuilder, "alice");
        VARCHAR.writeString(expectedBlockBuilder, "bob");
        VARCHAR.writeString(expectedBlockBuilder, "charlie");
        VARCHAR.writeString(expectedBlockBuilder, "dave");
        VARCHAR.writeString(expectedBlockBuilder, "alice");
        VARCHAR.writeString(expectedBlockBuilder, "bob");
        VARCHAR.writeString(expectedBlockBuilder, "charlie");
        VARCHAR.writeString(expectedBlockBuilder, "dave");
        Block expectedBlock = expectedBlockBuilder.build();

        assertBlockEquals(VARCHAR, actualBlock, expectedBlock);
    }
}

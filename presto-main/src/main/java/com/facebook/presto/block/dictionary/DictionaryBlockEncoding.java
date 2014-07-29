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
package com.facebook.presto.block.dictionary;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.block.BlockEncodingFactory;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Preconditions;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import static com.google.common.base.Preconditions.checkNotNull;

public class DictionaryBlockEncoding
        implements BlockEncoding
{
    public static final BlockEncodingFactory<DictionaryBlockEncoding> FACTORY = new DictionaryBlockEncodingFactory();
    private static final String NAME = "DIC";

    private final Block dictionary;
    private final BlockEncoding idBlockEncoding;

    public DictionaryBlockEncoding(Block dictionary, BlockEncoding idBlockEncoding)
    {
        this.dictionary = checkNotNull(dictionary, "dictionary is null");
        this.idBlockEncoding = checkNotNull(idBlockEncoding, "idBlockEncoding is null");
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public Type getType()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block)
    {
        DictionaryEncodedBlock dictionaryBlock = (DictionaryEncodedBlock) block;
        Preconditions.checkArgument(dictionaryBlock.getDictionary() == dictionary, "Block dictionary is not the same a this dictionary");
        idBlockEncoding.writeBlock(sliceOutput, dictionaryBlock.getIdBlock());
    }

    @Override
    public Block readBlock(SliceInput sliceInput)
    {
        Block idBlock = idBlockEncoding.readBlock(sliceInput);
        return new DictionaryEncodedBlock(dictionary, idBlock);
    }

    private static class DictionaryBlockEncodingFactory
            implements BlockEncodingFactory<DictionaryBlockEncoding>
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public DictionaryBlockEncoding readEncoding(TypeManager manager, BlockEncodingSerde serde, SliceInput input)
        {
            // read the dictionary
            BlockEncoding dictionaryEncoding = serde.readBlockEncoding(input);
            Block dictionary = dictionaryEncoding.readBlock(input);

            // read the id block encoding
            BlockEncoding idBlockEncoding = serde.readBlockEncoding(input);
            return new DictionaryBlockEncoding(dictionary, idBlockEncoding);
        }

        @Override
        public void writeEncoding(BlockEncodingSerde serde, SliceOutput output, DictionaryBlockEncoding blockEncoding)
        {
            // write the dictionary
            BlockEncoding dictionaryBlockEncoding = blockEncoding.dictionary.getEncoding();
            serde.writeBlockEncoding(output, dictionaryBlockEncoding);
            dictionaryBlockEncoding.writeBlock(output, blockEncoding.dictionary);

            // write the id block encoding
            serde.writeBlockEncoding(output, blockEncoding.idBlockEncoding);
        }
    }
}

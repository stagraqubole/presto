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
package com.facebook.presto.raptor;

import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;

public class NoColumnsPageSource
        implements ConnectorPageSource
{
    private final Iterator<Integer> positionCounts;
    private boolean closed;

    public NoColumnsPageSource(Iterable<Integer> positionCounts)
    {
        this.positionCounts = checkNotNull(positionCounts, "positionCounts is null").iterator();
    }

    @Override
    public long getTotalBytes()
    {
        return 0;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public void close()
    {
        closed = true;
    }

    @Override
    public boolean isFinished()
    {
        return closed;
    }

    @Override
    public Page getNextPage()
    {
        if (closed) {
            return null;
        }

        if (!positionCounts.hasNext()) {
            closed = true;
            return null;
        }

        return new Page(positionCounts.next());
    }
}

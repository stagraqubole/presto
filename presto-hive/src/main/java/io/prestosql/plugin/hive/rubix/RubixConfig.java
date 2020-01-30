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
package io.prestosql.plugin.hive.rubix;

import io.airlift.configuration.Config;

public class RubixConfig
{
    boolean cacheEnabled;
    boolean parallelWarmupEnabled = true;
    String cacheLocation = "/tmp";

    public boolean isCacheEnabled()
    {
        return cacheEnabled;
    }

    public boolean isParallelWarmupEnabled()
    {
        return parallelWarmupEnabled;
    }

    @Config("hive.cache.enabled")
    public RubixConfig setCacheEnabled(boolean value)
    {
        this.cacheEnabled = value;
        return this;
    }

    @Config("hive.cache.cache.parallel.warmup")
    public RubixConfig setParallelWarmupEnabled(boolean value)
    {
        this.parallelWarmupEnabled = value;
        return this;
    }

    public String getCacheLocation()
    {
        return cacheLocation;
    }

    @Config("hive.cache.cache.location")
    public RubixConfig setCacheLocation(String location)
    {
        this.cacheLocation = location;
        return this;
    }
}

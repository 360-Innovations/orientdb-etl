/*
 *
 *  * Copyright 2010-2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package com.orientechnologies.orient.etl.loader;

import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.OETLProcessor;

/**
 * ETL Loader that saves record into OrientDB database.
 */
public class OMemoryLoader extends OAbstractLoader {
  protected List<Object> loadedObjects;

  @Override
  public void load(final Object input, final OCommandContext context) {
    progress.incrementAndGet();
    loadedObjects.add(input);
  }

  public List<Object> getLoadedObjects() {
    return loadedObjects;
  }
  
  @Override
  public String getUnit() {
    return "bytes";
  }

  @Override
  public void rollback() {
  }

  @Override
  public ODocument getConfiguration() {
    return new ODocument();
  }

  @Override
  public String getName() {
    return "memory";
  }

  @Override
  public void configure(OETLProcessor iProcessor, ODocument iConfiguration, OBasicCommandContext iContext) {
    super.configure(iProcessor, iConfiguration, iContext);
    
    this.loadedObjects = new ArrayList<Object>();
  }
}

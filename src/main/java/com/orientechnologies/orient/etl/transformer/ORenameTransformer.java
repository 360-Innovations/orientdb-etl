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

package com.orientechnologies.orient.etl.transformer;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.OETLProcessor;

public class ORenameTransformer extends OAbstractTransformer {
  protected ODocument rename;

  @Override
  public ODocument getConfiguration() {
    return new ODocument().fromJSON("{parameters:[" + getCommonConfigurationParameters() + ","
        + "{fieldName:{optional:false,description:'field name to apply the result'}},"
        + "{expression:{optional:true,description:'expression to evaluate. Mandatory with operation=set (default)'}}"
        + "{renameTo:{optional:true,description:'the new name for the field'}}"
        + "{operation:{optional:false,description:'operation to execute against the field: set, remove. Default is set'}}" + "],"
        + "input:['ODocument'],output:'ODocument'}");
  }

  @Override
  public void configure(OETLProcessor iProcessor, final ODocument iConfiguration, OBasicCommandContext iContext) {
    super.configure(iProcessor, iConfiguration, iContext);

    rename = iConfiguration;
  }

  @Override
  public String getName() {
    return "rename";
  }

  @Override
  public Object executeTransform(final Object input) {
    if (rename != null && input instanceof OIdentifiable) {
      final ORecord rec = ((OIdentifiable) input).getRecord();

      if (rec instanceof ODocument) {
        final ODocument doc = (ODocument) rec;

        for (String fieldName : rename.fieldNames()) {
          doc.field((String) rename.field(fieldName), doc.field(fieldName));
          doc.removeField(fieldName);
        }
      }
    }

    return input;
  }
}

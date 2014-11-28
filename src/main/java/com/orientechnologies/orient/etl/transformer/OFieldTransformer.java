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
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;
import com.orientechnologies.orient.etl.OETLProcessor;

public class OFieldTransformer extends OAbstractTransformer {
  protected String     fieldName;
  protected String     expression;
  protected boolean    setOperation = true;
  protected OSQLFilter sqlFilter;
  protected Object     valueDefinition;

  @Override
  public ODocument getConfiguration() {
    return new ODocument().fromJSON("{parameters:[" + getCommonConfigurationParameters() + ","
        + "{fieldName:{optional:false,description:'field name to apply the result'}},"
        + "{expression:{optional:true,description:'expression to evaluate. Mandatory with operation=set (default)'}}"
        + "{operation:{optional:false,description:'operation to execute against the field: set, remove. Default is set'}}" + "],"
        + "input:['ODocument'],output:'ODocument'}");
  }

  @Override
  public void configure(OETLProcessor iProcessor, final ODocument iConfiguration, OBasicCommandContext iContext) {
    super.configure(iProcessor, iConfiguration, iContext);
    fieldName = (String) resolve(iConfiguration.field("fieldName"));
    expression = iConfiguration.field("expression");
    valueDefinition = iConfiguration.field("value");
    
    if (expression != null) 
      sqlFilter = new OSQLFilter(expression, context, null);
    
    if (iConfiguration.containsField("operation"))
      setOperation = "set".equalsIgnoreCase((String) iConfiguration.field("operation"));
  }

  @Override
  public String getName() {
    return "field";
  }

  @Override
  public Object executeTransform(final Object input) {
    Object value = valueDefinition;
    
    // Process ODocument value objects for ${} and $={}
    if (valueDefinition instanceof ODocument) {
      ODocument valueDoc = ((ODocument) valueDefinition).copy();
      
      for (String field : valueDoc.fieldNames()) {
    	  if(field.equals("@@class")){
    		  String className = (String) resolve(valueDoc.field("@@class"));
    		  valueDoc.field("@class", className);
    		  valueDoc.removeField("@@class");
    	  } else {
	        valueDoc.field(field, resolve(valueDoc.field(field)));        
    	  }
      }
      value = valueDoc;
    }
        
    if (input instanceof OIdentifiable) {
      final ORecord rec = ((OIdentifiable) input).getRecord();

      if (rec instanceof ODocument) {
        final ODocument doc = (ODocument) rec;

        if (setOperation) {
          if (value != null) {
            doc.field(fieldName, value);
          } 
          else {
            final Object newValue = sqlFilter.evaluate(doc, null, context);

            // SET THE TRANSFORMED FIELD BACK
            doc.field(fieldName, newValue);

            log(OETLProcessor.LOG_LEVELS.DEBUG, "set %s=%s in document=%s", fieldName, newValue, doc);
          }
        } else {
          final Object prev = doc.removeField(fieldName);

          log(OETLProcessor.LOG_LEVELS.DEBUG, "removed %s (value=%s) from document=%s", fieldName, prev, doc);
        }
      }
    }

    return input;
  }
}

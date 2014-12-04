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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.filter.OSQLFilter;
import com.orientechnologies.orient.etl.OETLProcessor;

/**
 * Converts a JOIN in LINK
 */
public class OETLTransformer extends OAbstractLookupTransformer {
	protected String fieldName;
	protected String fileName;
	protected String keyFieldName;
	protected String valueFieldName;
	protected ODocument parameters;
	protected String fieldType;
	private Map<String, OSQLFilter> sqlFilters;
	private OBasicCommandContext iContext;

	@Override
	public ODocument getConfiguration() {
		return new ODocument()
				.fromJSON("{parameters:["
						+ getCommonConfigurationParameters()
						+ ","
						+ "{joinFieldName:{optional:true,description:'field name containing the value to join'}},"
						+ "{joinValue:{optional:true,description:'value to use in lookup query'}},"
						+ "{linkFieldName:{optional:false,description:'field name containing the link to set'}},"
						+ "{linkFieldType:{optional:true,description:'field type containing the link to set. Use LINK for single link and LINKSET or LINKLIST for many'}},"
						+ "{lookup:{optional:false,description:'<Class>.<property> or Query to execute'}},"
						+ "{unresolvedLinkAction:{optional:true,description:'action when a unresolved link is found',values:"
						+ stringArray2Json(ACTION.values()) + "}}],"
						+ "input:['ODocument'],output:'ODocument'}");
	}

	@Override
	public void configure(OETLProcessor iProcessor,
			final ODocument iConfiguration, OBasicCommandContext iContext) {
		super.configure(iProcessor, iConfiguration, iContext);

		fieldName = iConfiguration.field("fieldName");
		fileName = (String) resolve(iConfiguration.field("fileName"));
		keyFieldName = iConfiguration.field("keyFieldName");
		valueFieldName = iConfiguration.field("valueFieldName");
		parameters = iConfiguration.field("parameters");
		fieldType = iConfiguration.field("fieldType");
		this.iContext = iContext;
	}

	@Override
	public String getName() {
		return "etl";
	}

	@Override
	public Object executeTransform(final Object input) {
		if (fileName == null || fileName.length() == 0) {
			throw new OConfigurationException(
					"fileName must be specified for etl");
		}
		ODocument doc = (ODocument) input;
		List<String> params = new ArrayList<String>();

		params.add(fileName);

		if (parameters != null) {
			if (sqlFilters == null) {
				sqlFilters = new HashMap<String, OSQLFilter>();
			}

			for (String name : parameters.fieldNames()) {
				if (!sqlFilters.containsKey(name)) {
					String formula = parameters.field(name);
					sqlFilters
							.put(name, new OSQLFilter(formula, context, null));
				}

				OSQLFilter sqlFilter = sqlFilters.get(name);
				String value = sqlFilter.evaluate((ODocument) input, null,
						context).toString();

				params.add('-' + name + '=' + value);
			}
		}

		for (Map.Entry<String, Object> entries : iContext.getVariables()
				.entrySet()) {
			params.add('-' + entries.getKey() + '=' + entries.getValue());
		}

		final List<Object> results = OETLProcessor.executeSubETL(params
				.toArray(new String[] {}));
		Object fieldValue = null;

		if (fieldType != null && fieldName != null) {
			if (fieldType.equalsIgnoreCase(OType.EMBEDDEDMAP.name())) {
				fieldValue = createEmbeddedMap(results);
				if (fieldValue != null) {
					doc.field(fieldName, fieldValue, OType.EMBEDDEDMAP);
				}
			} else if (fieldType.equalsIgnoreCase(OType.EMBEDDED.name())) {
				fieldValue = createEmbedded(results);
				if (fieldValue != null) {
					doc.field(fieldName, fieldValue, OType.EMBEDDED);
				}
			} else if (fieldType.equalsIgnoreCase(OType.EMBEDDEDSET.name())) {
				fieldValue = createEmbeddedset(results);
				if (fieldValue != null) {
					doc.field(fieldName, fieldValue, OType.EMBEDDEDSET);
				}
			} else if (fieldType.equalsIgnoreCase(OType.LINKLIST.name())) {
				fieldValue = createList(results);
				if (fieldValue != null) {
					doc.field(fieldName, fieldValue, OType.EMBEDDED);
				}
			}
		}

		return input;
	}

	private Object createEmbeddedset(List<Object> results) {
		// todo error processing
		return results;
	}

	private Object createList(List<Object> results) {
		List<String> list = new ArrayList<String>();

		for (Object result : results) {
			ODocument doc = (ODocument) result;
			list.add((String) doc.field(valueFieldName));
		}

		return list;
	}

	private Object createEmbedded(List<Object> results) {
		// todo error processing
		if (results.size() == 1) {
			return results.get(0);
		}

		return null;
	}

	private Object createEmbeddedMap(List<Object> results) {
		Map<String, Object> map = new HashMap<String, Object>();

		for (Object result : results) {
			ODocument doc = (ODocument) result;
			map.put((String) doc.field(keyFieldName), doc.field(valueFieldName));
		}

		return map;
	}
}

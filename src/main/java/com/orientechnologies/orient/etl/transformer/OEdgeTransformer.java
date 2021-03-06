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
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.etl.OETLProcessHaltedException;
import com.orientechnologies.orient.etl.OETLProcessor;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class OEdgeTransformer extends OAbstractLookupTransformer {
  protected OrientBaseGraph graph;
  protected String          edgeClass    = "E";
  protected boolean         directionOut = true;

  @Override
  public ODocument getConfiguration() {
    return new ODocument().fromJSON("{parameters:[" + getCommonConfigurationParameters() + ","
        + "{joinFieldName:{optional:false,description:'field name containing the value to join'}},"
        + "{lookup:{optional:false,description:'<Class>.<property> or Query to execute'}},"
        + "{direction:{optional:true,description:'Direction between \'in\' and \'out\'. Default is \'out\''}},"
        + "{class:{optional:true,description:'Edge class name. Default is \'E\''}},"
        + "{unresolvedVertexAction:{optional:true,description:'action when a unresolved vertices is found',values:"
        + stringArray2Json(ACTION.values()) + "}}]," + "input:['ODocument','OrientVertex'],output:'OrientVertex'}");
  }

  @Override
  public void configure(OETLProcessor iProcessor, final ODocument iConfiguration, final OBasicCommandContext iContext) {
    super.configure(iProcessor, iConfiguration, iContext);
    edgeClass = iConfiguration.field("class");

    if (iConfiguration.containsField("direction")) {
      final String direction = iConfiguration.field("direction");
      if ("out".equalsIgnoreCase(direction))
        directionOut = true;
      else if ("in".equalsIgnoreCase(direction))
        directionOut = false;
      else
        throw new OConfigurationException("Direction can be 'in' or 'out', but found: " + direction);

    }
  }

  @Override
  public String getName() {
    return "edge";
  }

  @Override
  public void begin() {
    if (graph == null) {
      graph = pipeline.getGraphDatabase();
      final OClass cls = graph.getEdgeType(edgeClass);
      if (cls == null)
        graph.createEdgeType(edgeClass);
    }

    super.begin();
  }

  @Override
  public Object executeTransform(final Object input) {
    // GET JOIN VALUE
    final OrientVertex vertex;
    if (input instanceof OIdentifiable)
      vertex = graph.getVertex(input);
    else if (input instanceof OrientVertex)
      vertex = (OrientVertex) input;
    else
      throw new OTransformException(getName() + ": input type '" + input + "' is not supported");

    Object joinValue = vertex.getProperty(joinFieldName);

    Object result = lookup(joinValue);

    log(OETLProcessor.LOG_LEVELS.DEBUG, "joinValue=%s, lookupResult=%s", joinValue, result);

    if (result == null) {
      // APPLY THE STRATEGY DEFINED IN unresolvedLinkAction
      switch (unresolvedLinkAction) {
      case CREATE:
        if (lookup != null) {
          final String[] lookupParts = lookup.split("\\.");
          final OrientVertex linkedV = graph.addTemporaryVertex(lookupParts[0]);
          linkedV.setProperty(lookupParts[1], joinValue);
          linkedV.save();

          log(OETLProcessor.LOG_LEVELS.DEBUG, "created new vertex=%s", linkedV.getRecord());

          result = linkedV;
        } else
          throw new OConfigurationException("Cannot create linked document because target class is unknown. Use 'lookup' field");
        break;
      case ERROR:
        processor.getStats().incrementErrors();
        log(OETLProcessor.LOG_LEVELS.ERROR, "%s: ERROR Cannot resolve join for value '%s'", getName(), joinValue);
        break;
      case WARNING:
        processor.getStats().incrementWarnings();
        log(OETLProcessor.LOG_LEVELS.INFO, "%s: WARN Cannot resolve join for value '%s'", getName(), joinValue);
        break;
      case SKIP:
        return null;
      case HALT:
        throw new OETLProcessHaltedException("Cannot resolve join for value '" + joinValue + "'");
      }
    }

    if (result != null) {
      final OrientVertex targetVertex = graph.getVertex(result);

      // CREATE THE EDGE
      final OrientEdge edge;
      if (directionOut)
        edge = (OrientEdge) vertex.addEdge(edgeClass, targetVertex);
      else
        edge = (OrientEdge) targetVertex.addEdge(edgeClass, vertex);

      log(OETLProcessor.LOG_LEVELS.DEBUG, "created new edge=%s", edge);
    }

    return input;
  }
}

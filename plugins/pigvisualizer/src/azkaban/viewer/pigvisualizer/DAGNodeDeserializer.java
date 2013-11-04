package azkaban.viewer.pigvisualizer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;

import com.twitter.ambrose.model.DAGNode;

public class DAGNodeDeserializer extends StdScalarDeserializer<DAGNode> {
	public DAGNodeDeserializer() {
		super(DAGNode.class);
	}

	@Override
	public DAGNode<T> deserialize(JsonParser parser,
			DeserializationContext context)
			throws IOException, JsonProcessingException {

		if (parser.getCurrentToken() == JsonToken.VALUE_STRING) {
			String value = parser.getText();
			DAGNode<T> node = DAGNode.fromJson(value);
			return node;
		} else {
			throw context.mappingException(getValueClass(), 
					parser.getCurrentToken());
	}
}

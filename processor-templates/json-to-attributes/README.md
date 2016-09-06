# How to Setup Example

1. Updload discover-json-to-attributes.xml
2. Drag template onto canvas
3. Start all processors. May have to change PutElasticsearch configs depending on how it is running. Disabling it okay as well since we are also putting the flow file to local file.
4. Make directory /json-to-attributes
5. Copy test-input.json to /json-to-attributes
6. The flow file's attributes will be converted to json and outputed to /json-attributes.
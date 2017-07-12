# DynamoNodeSlides
Slides for the talk "Leveraging DynamoDB in a Node.js environment"

## Express Example
Most of the value from this can just come from looking at [the code](./examples/dynamonode), but here's some instructions to run the project if you'd like. 

1. Clone and install dependencies:
```
git clone https://github.com/rbickham11/DynamoNodeSlides.git
cd DynamoNodeSlides/examples/dynamonode
npm install
```

2. Create a file called `config.json` in the `examples/dynamonode/dynamo` directory with the following:
```
{
  "accessKey": "YOUR_ACCESS_KEY",
  "secretKey": "YOUR_SECRET KEY",
  "region": "YOUR_REGION"
}
```

3. Run `npm start`

To use the project as is, you must have a table in your dynamo account named `dynamotalk_people` with `location` as the partition key and `id` as the sort key. 

However, to use a different table set you can modify the created tables [here](./examples/dynamonode/dynamo/tablecollection.js#L48). Simply add or remove function calls similar to that on line 48.

You can also modify the searched prefix [here](./examples/dynamonode/bin/www#L75)

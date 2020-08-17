# Heads Up! 

This library/repository is no longer being maintained. As such, we don't recommend using this in production, but we have left the code-base here for posterity and for those of you who would like to clone the code and maintain it yourself. 


---


# loopback-connector-rethinkdb

Loopback connector for RethinkDB.

## Installation

In your application root directory, enter this command to install the connector:

```sh
npm install loopback-connector-rethinkdb --save
```

This installs the module from npm and adds it as a dependency to the application's `package.json` file.

## Creating a RethinkDB data source

Add an entry in the application's `/server/datasources.json` :

```javascript
"mydb": {
  "url":  "http://username:password@host.com:28015/dbName"
  "connector": "rethinkdb"  
}
```

Edit `datasources.json` to add any other additional properties that you require.

### Connection properties

| Property | Type&nbsp;&nbsp; | Description |
| --- | --- | --- |
| connector | String | Connector name, “loopback-connector-rethinkdb”|
| url | String | Full connection url. Overrides other connection settings |
| database | String | Database name |
| host | String | Database host name |
| password | String | Password to connect to database |
| port | Number | Database TCP port |
| username | String | Username to connect to database |

### Additional properties

You can specify an 'additionalSettings' property: 
```json
"additionalSettings": {
  "ssl": {
    "ca": "${RETHINKDB_SSL}"
  }
}
``````
`RETHINKDB_SSL` contains the base64encoded SSL cert.


### Changefeed
The connector has support for RethinkDB changefeeds.
You can access to the functionality by doing:
```javascript
ModelName.dataSource.connector.changeFeed(Model.modelName, filter, options);
```
The `filter` parameter is the standard loopback filtering object.
The `options` parameter can have a `throttle` property, specified in milliseconds. Disabled by default.


PS: If a change happens, the changefeed function will return the entire results set.
Delta results are on the roadmap

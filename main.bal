
import ballerina/log;
import ballerinax/java.jdbc;
import ballerina/sql;
import ballerina/http;
import ballerina/mime;

type Country record {
    int id;
    string alpha2;
    string alpha3;
    string name;
};

jdbc:Client countryDBClient = check initDatabase();

http:Client flagEndpoint = check new ("https://flagcdn.com");

function initDatabase() returns error|jdbc:Client {
    jdbc:Client|sql:Error dbClient = new ("jdbc:h2:./data/countriesdb");

    if dbClient is jdbc:Client {

        sql:ExecutionResult|sql:Error droptable = dbClient->execute(`DROP TABLE IF EXISTS countries`);
        if droptable is error {
            log:printError("unable to drop the table", errorMsg = droptable.message());
            return droptable;
        }

        sql:ExecutionResult|sql:Error loadData = dbClient->execute(`CREATE TABLE countries(id INT PRIMARY KEY, alpha2 VARCHAR(255), alpha3 VARCHAR(255), name VARCHAR(255)) AS SELECT * FROM CSVREAD('classpath:/resources/ramithjayasingheznszn/dddss/1/countries.csv')`);
        if loadData is error {
            log:printError("unable to drop the table", errorMsg = loadData.message());
            return loadData;
        }

        log:printInfo("loaded countries");

        stream<Country, sql:Error?> resultStream = dbClient->query(`SELECT * FROM countries`);
        // Iterating the returned table.
        check from Country country in resultStream
            do {
                log:printInfo(country.toString());
            };
    }

    return dbClient;
}

service / on new http:Listener(9090) {

    resource function get country/[string code]() returns Country|error? {
        Country queryRowResponse = check countryDBClient->queryRow(`SELECT id, alpha2, alpha3, name from countries WHERE alpha2 = ${code} OR alpha3 =${code}`);
        return queryRowResponse;
    }

    resource function get country/[string code]/flag(http:Request request, http:Caller caller) returns error? {
        Country queryRowResponse = check countryDBClient->queryRow(`SELECT id, alpha2, alpha3, name from countries WHERE alpha2 = ${code} OR alpha3 =${code}`);

        byte[]|error content = getFlag(queryRowResponse.alpha2);
        http:Response response = new ();

        if content is error {
            response.statusCode = http:STATUS_INTERNAL_SERVER_ERROR;
            response.setTextPayload(content.message(), mime:TEXT_PLAIN);
        } else {
            response.statusCode = http:STATUS_OK;
            response.setBinaryPayload(content, mime:IMAGE_PNG);

        }

        check caller->respond(response);
    }

}

function getFlag(string countryCode) returns byte[] | error {
    http:Response res = check flagEndpoint->get("/80x60/" + countryCode + ".png" );
    return check res.getBinaryPayload();
}


// isolated function getFlag(string countryCode) returns byte[]|error = @java:Method {
//     name: "getFlag",
//     'class: "lk.opensource.ramithj.flags.FlagReader"
// } external;
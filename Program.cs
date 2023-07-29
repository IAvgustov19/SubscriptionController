using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Text.Json;
using Ydb.Sdk;
using Ydb.Sdk.Value;
using Ydb.Sdk.Auth;
using Ydb.Sdk.Table;
using ICredentialsProvider = Ydb.Sdk.Auth.ICredentialsProvider;
using Yandex.Cloud.Functions;
using Newtonsoft.Json;
using TimelineLibrary;
using ResultSet = Ydb.Sdk.Value.ResultSet;
using MimeKit;
using MailKit.Net.Smtp;
using Microsoft.Extensions.Logging;
using static Yandex.Cloud.Mdb.Clickhouse.V1.Config.ClickhouseConfig.Types.ExternalDictionary.Types.Structure.Types;
using Org.BouncyCastle.Utilities.IO.Pem;
using Ydb;
using static Ydb.Sdk.Value.ResultSet;
using System.Security.AccessControl;

namespace Function;

public class Request
{
    public string httpMethod { get; set; }
    public string body { get; set; }
}

public class Response
{
    public Response(int statusCode, string body)
    {
        StatusCode = statusCode;
        Body = body;
    }

    public int StatusCode { get; set; }
    public string Body { get; set; }
}

public enum ExpiredDateType
{
    ThreeDay, OneDay
}

public class Handler
{
    private ResultSet? resultSet;
    public async Task<Response> FunctionHandler(Request request, Context context)
    {
        string? endpoint = Environment.GetEnvironmentVariable("END_POINT");
        string? database = Environment.GetEnvironmentVariable("DATA_BASE");

        Token? token = JsonConvert.DeserializeObject<Token>(context.TokenJson);

        ICredentialsProvider credentialsProvider = new TokenProvider(token.Access_token);

        var config = new DriverConfig(
           endpoint: endpoint,
           database: database,
           credentials: credentialsProvider
       );

        using var driver = new Driver(config);
        await driver.Initialize();
        using var tableClient = new TableClient(driver, new TableClientConfig());
        Ydb.Sdk.Client.IResponse response;
        ExecuteDataQueryResponse? queryResponse;
        Random ran = new Random();

        //Убирать подписку если срок вышел
        //Сначала проверяеться у кого закончилась подписка, а потом добавляю, кто попал под это
        response = await tableClient.SessionExec(async session =>
        {
            var query = @"
        DECLARE $date AS Date;

        SELECT ID FROM accounts VIEW ExpiredDateIndex
        WHERE ExpiredDate <= $date";

            return await session.ExecuteDataQuery(
                query: query,
                txControl: TxControl.BeginSerializableRW().Commit(),
                parameters: new Dictionary<string, YdbValue>
                {
                    { "$date", YdbValue.MakeDate(DateTime.Now) },
                }
            );
        });

        response.Status.EnsureSuccess();
        queryResponse = (ExecuteDataQueryResponse)response;
        resultSet = queryResponse.Result.ResultSets[0];

        for (int i = 0; i < resultSet.Rows.Count; i++)
        {
            response = await tableClient.SessionExec(async session =>
            {
                var query = $@"
        DECLARE $id AS Uint64;

        UPSERT INTO accounts (ID, ExpiredDate, Subscription) VALUES ($id, DATE({new string("\"1970-01-01\"")}), false)";

                return await session.ExecuteDataQuery(
                    query: query,
                    txControl: TxControl.BeginSerializableRW().Commit(),
                    parameters: new Dictionary<string, YdbValue>
                    {
                        { "$id", YdbValue.MakeUint64((ulong)resultSet.Rows[i][0].GetOptionalUint64()) },
                    }
                );
            });
            response.Status.EnsureSuccess();
        }

        //Update FreeEvents amount
        response = await tableClient.SessionExec(async session =>
        {
            var query = @"

        SELECT ID FROM accounts VIEW SubIndex
        WHERE Subscription == false";

            return await session.ExecuteDataQuery(
                query: query,
                txControl: TxControl.BeginSerializableRW().Commit(),
                parameters: new Dictionary<string, YdbValue>()
            );
        });

        response.Status.EnsureSuccess();
        queryResponse = (ExecuteDataQueryResponse)response;
        resultSet = queryResponse.Result.ResultSets[0];

        for (int i = 0; i < resultSet.Rows.Count; i++)
        {
            response = await tableClient.SessionExec(async session =>
            {
                var query = @"
        DECLARE $id AS Uint64;
        DECLARE $countFree AS Uint8;

        UPSERT INTO accounts (ID, CountFreeEvents) VALUES ($id, $countFree)";

                return await session.ExecuteDataQuery(
                    query: query,
                    txControl: TxControl.BeginSerializableRW().Commit(),
                    parameters: new Dictionary<string, YdbValue>
                    {
                        { "$id", YdbValue.MakeUint64((ulong)resultSet.Rows[i][0].GetOptionalUint64()) },
                        { "$countFree", YdbValue.MakeUint8(3) },
                    }
                );
            });
            response.Status.EnsureSuccess();
        }

        return new Response(200, "Hello, world!");
    }
    public static void Main() { }
}
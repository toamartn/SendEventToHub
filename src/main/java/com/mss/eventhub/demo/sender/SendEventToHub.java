package com.mss.eventhub.demo.sender;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.PartitionSender;
import com.microsoft.azure.eventhubs.EventHubException;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

public class SendEventToHub {

    public static void main(String[] args) throws EventHubException, ExecutionException, InterruptedException, IOException {

        final ConnectionStringBuilder connStr = new ConnectionStringBuilder()
                .setNamespaceName("mss-**")	//	event hubs / Event Hubs Namespace
                .setEventHubName("mss-**")	//	name of event hub
                .setSasKeyName("SAS-**")	//	create Shared access policies under event hub. Send/listen or both.
                .setSasKey("*****");	//	shared access policies key

        final Gson gson = new GsonBuilder().create();
        final PayloadEvent payload = new PayloadEvent("5AC", "Hello Event to Event Hub ", 1);	//	local class containing sample data.
        byte[] payloadBytes = gson.toJson(payload).getBytes(Charset.defaultCharset());	//	converting payload into byte array.
        final EventData sendEvent = EventData.create(payloadBytes);	//	prapareing event data from the byte array.
        final ExecutorService executorService = Executors.newSingleThreadExecutor();

        final EventHubClient ehClient = EventHubClient.createSync(connStr.toString(), executorService);
        PartitionSender sender = null;

        try {
            ehClient.sendSync(sendEvent);
            final String partitionKey = "partitionTheStream";
            ehClient.sendSync(sendEvent, partitionKey);
            sender = ehClient.createPartitionSenderSync("0");
            sender.sendSync(sendEvent);
            System.out.println(Instant.now() + ": Send Complete...");
            System.in.read();
        } finally {
            if (sender != null) {
                sender.close().thenComposeAsync(aVoid -> ehClient.close(), executorService)
                				.whenCompleteAsync((aVoid1, throwable) -> 
                					{
			                            if (throwable != null) {
			                                System.out.println(String.format("closing failed with error: %s", throwable.toString()));
			                            }
			                        },
                					executorService).get();
            } else {// This cleans up the resources including any open sockets
                ehClient.closeSync();
            }
            executorService.shutdown();
        }
    }

    static final class PayloadEvent {
    	
        PayloadEvent(final String messageID, final String message, int seed) {
            this.id = messageID;
            this.strProperty = message;
            this.longProperty = seed * new Random().nextInt(seed);
            this.intProperty = seed * new Random().nextInt(seed);
        }

        public String id;
        public String strProperty;
        public long longProperty;
        public int intProperty;
    }
}
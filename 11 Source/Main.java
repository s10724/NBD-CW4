package com.company;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.cap.UnresolvedConflictException;
import com.basho.riak.client.api.commands.kv.DeleteValue;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.kv.UpdateValue;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;

import java.util.concurrent.ExecutionException;

public class Main {



    private static void addObjectToDatabase(RiakClient client, Namespace bucket) throws ExecutionException, InterruptedException {
        Person person = new Person("Adam", "Iksinski", 25);
        Location personLocation = new Location(bucket, "Iksinski");
        StoreValue store = new StoreValue.Builder(person).withLocation(personLocation).build();
        StoreValue.Response response = client.execute(store);
        System.out.println("Person added");
    }

    private static void downloadAndPrintObjectFromDatabase(RiakClient client, Namespace bucket) throws ExecutionException, InterruptedException {
        Location location = new Location(bucket, "Iksinski");
        FetchValue fv = new FetchValue.Builder(location).build();
        FetchValue.Response response = client.execute(fv);
        Person person = response.getValue(Person.class);
        //System.out.println(response);
        System.out.println(person);
    }


    private static void updateObjectInDatabase(RiakClient client, Namespace bucket) throws ExecutionException, InterruptedException {
        Person person = new Person("Adam", "Iksinski", 30);
        Location personLocation = new Location(bucket, "Iksinski");
        PersonUpdate personUp = new PersonUpdate(person);
        UpdateValue updateValue = new UpdateValue.Builder(personLocation).withUpdate(personUp).build();
        UpdateValue.Response response = client.execute(updateValue);
        System.out.println("Person updated");

    }


    private static void deleteObjectInDatabase(RiakClient client, Namespace bucket) throws ExecutionException, InterruptedException {
        Location personLocation = new Location(bucket, "Iksinski");
        DeleteValue deletePerson = new DeleteValue.Builder(personLocation).build();
        client.execute(deletePerson);
        System.out.println("Person deleted");
    }
        public static void main(String[] args) {
        RiakNode node = new RiakNode.Builder().withRemoteAddress("127.0.0.1").withRemotePort(8087).build();
        RiakCluster cluster = new RiakCluster.Builder(node).build();
        cluster.start();
        RiakClient client = new RiakClient(cluster);
        Namespace bucket = new Namespace("s10724");

        try {
            addObjectToDatabase(client, bucket);
            downloadAndPrintObjectFromDatabase(client, bucket);

            updateObjectInDatabase(client, bucket);
            downloadAndPrintObjectFromDatabase(client, bucket);

            deleteObjectInDatabase(client,bucket);
            downloadAndPrintObjectFromDatabase(client, bucket);
        } catch (Exception e) {
            e.printStackTrace();
        }


        cluster.shutdown();
    }
}

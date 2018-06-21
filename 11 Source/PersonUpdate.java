package com.company;

import com.basho.riak.client.api.commands.kv.UpdateValue;

public class PersonUpdate extends UpdateValue.Update<Person> {
    private final Person update;

    public PersonUpdate(Person update) {
        this.update = update;
    }

    @Override
    public Person apply(Person original) {
        if (original == null) {
            original = new Person();
        }

        original.setName(update.getName());
        original.setSurname(update.getSurname());
        original.setAge(update.getAge());
        return original;
    }
}
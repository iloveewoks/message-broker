package org.example.messages;

public class Message<ID, T> {

    private final ID id;
    private final T content;

    public Message(ID id, T content) {
        this.id = id;
        this.content = content;
    }

    public ID getId() {
        return id;
    }

    public T getContent() {
        return content;
    }

    @Override
    public String toString() {
        return "Message{" +
                "id=" + id +
                ", content=" + content +
                '}';
    }
}

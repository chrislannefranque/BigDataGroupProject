package nl.uva.bigdata.hadoop.assignment1;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class BooksWritable  implements Writable {

    private Book[] books;

    public void setBooks(Book[] books) { //set function is to update something; in this case, we're updating the books to a new array/ initializing
                                        // this is needed because the book array is private and therefore cannot be changed from within functions
        this.books = books;
    }
    //The problem with arrays in Java: not dynamic; when initializing, you have to specify the size, so that Java can allocate the appropriate amount of memory

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO Implement me
        // take books and transform to bytws

        out.writeInt(books.length); //first outputting the length of the array

        for (int i = 0; i < books.length; i++) {
            Book book = books[i];
            String title = book.getTitle();
            int year = book.getYear();

            out.writeUTF(title); //preserves formatting to avoid further errors
            out.writeInt(year);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO Implement me

        int array_length = in.readInt();
        this.books = new Book[array_length]; // initializing the array: requires inputting the length

        for (int i=0; i < array_length; i++) {
            String title = in.readUTF();
            int year = in.readInt();
            Book temp_book = new Book(title, year);
            this.books[i] = temp_book;
        }
    }

            @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BooksWritable that = (BooksWritable) o;
        return Arrays.equals(books, that.books);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(books);
    }
}

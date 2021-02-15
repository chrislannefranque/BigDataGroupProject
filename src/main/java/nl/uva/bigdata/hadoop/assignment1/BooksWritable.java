package nl.uva.bigdata.hadoop.assignment1;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class BooksWritable  implements Writable {

    private Book[] books;

    public void setBooks(Book[] books) {
        this.books = books;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO Implement me
        // Take books and transform to bits

        //1) Loop over the books array / dictinary
        
        //2) For
        //out.writeBytes(this.books.title.toString()); --> 101....01
        //out.writeBytes(this.books.year.toString()); --> 010

        for (int i = 0; i < books.length; i++) {​​

            Book book = books[i];
            
            String s = "{​​" + book.getTitle() + "_" + book.getYear() + "}​​"; // _
            out.writeBytes(s); --> 0101010101 + 0101010101010 + 0010101
            }​​

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO Implement me
        //read buffer (lebgth = 3 ) 101
        //https://docs.oracle.com/javase/7/docs/api/java/io/DataInput.html#readChar()
        
        //1) Read DataInput in while is not finished
        title = ''
        year = ''
        
        While (character = in.readChar()){
            if character = '{'
            // Next characters are the tittle
                title = title + character
            if character = '_'
            // next characters are the year
                year = year + character
            if character = '}'
                temp_book = New Book(title, year)
                this.Book.append(temp_book)
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

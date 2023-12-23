select * from book where  copies>3000 and publisher_id=101085
select * from book where  copies>3000 and publisher_id=101085
insert into customer values (315004,"Santa Claus","M"); 
insert into book values (290002,"Defence Against Dark Arts:","Jennifer Kanye",104379,2000);
insert into book values (290001, ¡°Defence Against Dark Arts¡±, ¡°Jennifer Kanye¡±, 120000, 2000); 
delete from customer where id >310200; 
delete from book where title = "Meine Juden--eure Juden"; 


select customer.name, book.title, orders.quantity from customer, book, orders where customer.id=orders.customer_id and book.id=orders.book_id and customer.gender="M" and book.copies>8000 and orders.quantity<>0  
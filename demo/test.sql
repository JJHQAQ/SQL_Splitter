2.1 select * from customer ;
2.2 select title from book ;
2.3 select * from book where copies>3000 and publisher_id=101085;
2.4 select book_id, quantity  from orders 
2.5 select book.title, book.authors from book, publisher where book.publisher_id=publisher.id and publisher.name="Twomorrows Publishing" and book.copies>6000 
2.6 select customer.name from customer, orders where customer.id=orders.customer_id 
2.7 select customer.name, book.title, orders.quantity from customer, book, orders where customer.id=orders.customer_id and book.id=orders.book_id and customer.gender="M" and book.copies>8000 and orders.quantity<>0  

1. insert into customer values (315004,"Santa Claus","M"); 
2.insert into book values (290002,"Defence Against Dark Arts:","Jennifer Kanye",104379,2000);
3.insert into book values (290001, "Defence Against Dark Arts", "Jennifer Kanye", 120000, 2000); 
4.insert into book values (290001, "Defence Against Dark Arts", "Jennifer Kanye", 120000,2000);  // 违反参照完整性约束

1.delete from customer where id >310200; 
2.delete from book where title = "Meine Juden--eure Juden"; 
3.delete from publisher where id=104379;  // 提示级联删除

关闭站点1、2 可以尝试对book表插入, 


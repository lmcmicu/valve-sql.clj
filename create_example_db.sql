---------------------------------------------
drop table if exists `breads`;

create table `breads` (
  `name` text
);

insert into `breads` (`name`)
values ('panini'),
       ('baguette'),
       ('whole wheat'),
       ('pumpernickel'),
       ('wonder');

---------------------------------------------
drop table if exists `available_breads`;

create table `available_breads` (
  `name` text
);

insert into `available_breads` (`name`)
values ('panini'),
       ('baguette'),
       ('whole wheat'),
       ('pumpernickel'),
       ('wonder');

---------------------------------------------
drop table if exists `vegetables`;

create table `vegetables` (
  `name` text
);

insert into `vegetables` (`name`)
values ('brussels sprouts'),
       ('cauliflower'),
       ('spinach'),
       ('carrots'),
       ('beets');

---------------------------------------------
drop table if exists `available_vegetables`;

create table `available_vegetables` (
  `name` text
);

insert into `available_vegetables` (`name`)
values ('brussels sprouts'),
       ('cauliflower'),
       ('spinach'),
       ('carrots'),
       ('beets');

---------------------------------------------
drop table if exists `pastas`;

create table `pastas` (
  `name` text
);

insert into `pastas` (`name`)
values ('farfalle'),
       ('penne'),
       ('spaghetti'),
       ('fetuccine'),
       ('rigatoni'),
       ('gemeli');

---------------------------------------------
drop table if exists `available_pastas`;

create table `available_pastas` (
  `name` text
);

insert into `available_pastas` (`name`)
values ('farfalle'),
       ('penne'),
       ('spaghetti'),
       ('fetuccine'),
       ('rigatoni'),
       ('gemeli');

---------------------------------------------
drop table if exists `soups`;

create table `soups` (
  `name` text
);

insert into `soups` (`name`)
values ('leek and potato'),
       ('cream of celery'),
       ('Italian wedding'),
       ('tomato');

---------------------------------------------
drop table if exists `available_soups`;

create table `available_soups` (
  `name` text
);

insert into `available_soups` (`name`)
values ('leek and potato'),
       ('cream of celery'),
       ('Italian wedding'),
       ('tomato');

---------------------------------------------
drop table if exists `meats`;

create table `meats` (
  `name` text
);

insert into `meats` (`name`)
values ('prime rib'),
       ('filet mignon'),
       ('lamb chops'),
       ('veal shank');

---------------------------------------------
drop table if exists `available_meats`;

create table `available_meats` (
  `name` text
);

insert into `available_meats` (`name`)
values ('prime rib'),
       ('filet mignon'),
       ('lamb chops'),
       ('veal shank');

---------------------------------------------
drop table if exists `veggie_mains`;

create table `veggie_mains` (
  `name` text
);

insert into `veggie_mains` (`name`)
values ('vegan curry'),
       ('fried jackfruit'),
       ('portobello burgers'),
       ('vegan chili'),
       ('vegetarian tacos');

---------------------------------------------
drop table if exists `available_veggie_mains`;

create table `available_veggie_mains` (
  `name` text
);

insert into `available_veggie_mains` (`name`)
values ('vegan curry'),
       ('fried jackfruit'),
       ('portobello burgers'),
       ('vegan chili'),
       ('vegetarian tacos');

---------------------------------------------
drop table if exists `desserts`;

create table `desserts` (
  `name` text
);

insert into `desserts` (`name`)
values ('tiramisu'),
       ('sundae'),
       ('ice cream scoop'),
       ('cheesecake'),
       ('chocolate cake'),
       ('brownies');

---------------------------------------------
drop table if exists `available_desserts`;

create table `available_desserts` (
  `name` text
);

insert into `available_desserts` (`name`)
values ('tiramisu'),
       ('sundae'),
       ('ice cream scoop'),
       ('cheesecake'),
       ('chocolate cake'),
       ('brownies');

---------------------------------------------
drop table if exists `meals`;

create table `meals` (
  `wedding_couple` text,
  `soup` text,
  `first_course` text,
  `second_course` text,
  `bread` text,
  `side` text,
  `dessert` text
);

insert into `meals`
            (`wedding_couple`,   `soup`,             `first_course`,  `second_course`, `bread`,        `side`,             `dessert`)
values      ('Mork and Mindy',   'leek and potato',  'spaghetti',     'Ork burgers',   'panini',       'spinach',          'sundae'),
            ('John and Nancy',   'leek and potato',  'penne',         'veal shank',    'wonder',       'cauliflower',      'tiramisu'),
            ('San and Diane',    'tomato',           'rigatoni',      'vegan chili',   'baguette',     'spinach',          'cheesecake'),
            ('Harry and Victor', 'cream of spinach', 'farfalle',      'filet mignon',  'pumpernickel', 'vegetarian tacos', 'chocolate cake');


------------------------------------------
drop table if exists `conditions`;

create table `conditions` (
  `table` text,
  `column` text,
  `condition` text
);

insert into `conditions` (`table`, `column`, `condition`)
values ('meals', 'soup', 'all(in(soups.name), in(available_soups.name))'),
       ('meals', 'first_course', 'all(in(pastas.name), in(available_pastas.name))'),
       ('meals', 'second_course', 'all(in(meats.name), in(available_meats.name))'),
       ('meals', 'bread', 'all(in(breads.name), in(available_breads.name))'),
       ('meals', 'side', 'all(in(vegetables.name), in(available_vegetables.name))'),
       ('meals', 'dessert', 'all(in(desserts.name), in(available_desserts.name))');

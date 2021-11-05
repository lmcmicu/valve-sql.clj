drop table if exists `twentieth_century_philosophers`;

create table `twentieth_century_philosophers` (
    `name` text,
    `tradition` text,
    `nationality` text
);

insert into `twentieth_century_philosophers`
values ('Moritz Schlick', 'Analytic', 'German');

insert into `twentieth_century_philosophers`
values ('Rudolf Carnap', 'Analytic', 'German');

insert into `twentieth_century_philosophers`
values ('Otto Neurath', 'Analytic', 'Austrian');

insert into `twentieth_century_philosophers`
values ('Friedrich Waismann', 'Analytic', 'Austrian');

insert into `twentieth_century_philosophers`
values ('Martin Heidegger', 'Continental', 'German');

insert into `twentieth_century_philosophers`
values ('A. J. Ayer', 'Analytic', 'English');

insert into `twentieth_century_philosophers`
values ('J. P. Sartre', 'Continental', 'French');

insert into `twentieth_century_philosophers`
values ('Maurice Merleau-Ponty', 'Continental', 'French');

insert into `twentieth_century_philosophers`
values ('Judith Butler', 'Continental', 'American');

insert into `twentieth_century_philosophers`
values ('W. V. O. Quine', 'Analytic', 'American');

insert into `twentieth_century_philosophers`
values ('Ludwig Wittgenstein', 'Mixed', 'Austrian');

------------------------------------------

drop table if exists `philosophers_covered_in_the_course`;

create table `philosophers_covered_in_the_course` (
    `name` text,
    `tradition` text,
    `nationality` text
);

insert into `philosophers_covered_in_the_course`
values ('Moritz Schlick', 'Analytic', 'German');

insert into `philosophers_covered_in_the_course`
values ('Rudolf Carnap', 'Analytic', 'German');

insert into `philosophers_covered_in_the_course`
values ('Otto Neurath', 'Analytic', 'Austrian');

insert into `philosophers_covered_in_the_course`
values ('Friedrich Waismann', 'Analytic', 'Austrian');

insert into `philosophers_covered_in_the_course`
values ('J. P. Sartre', 'Continental', 'French');

insert into `philosophers_covered_in_the_course`
values ('Maurice Merleau-Ponty', 'Continental', 'French');

------------------------------------------

drop table if exists `student_preferences_survey_results`;

create table `student_preferences_survey_results` (
    `student_name` text,
    `favourite_philosopher` text,
    `favourite_tradition` text
);

insert into `student_preferences_survey_results`
values ('Brunhilda', 'Friedrich Waismann', 'Analytic');

insert into `student_preferences_survey_results`
values ('Fredegund', 'Judith Butler', 'Continental');

insert into `student_preferences_survey_results`
values ('Jonathan', 'Immanuel Kant', 'Analytic');

insert into `student_preferences_survey_results`
values ('Harold', 'Rudolf Carnap', 'Continental');

insert into `student_preferences_survey_results`
values ('Cindy', 'Friedrich Waismann', 'Neither');

insert into `student_preferences_survey_results`
values ('Janet', 'Plato', 'Neither');

------------------------------------------
drop table if exists `conditions`;

create table `conditions` (
    `table` text,
    `column` text,
    `condition` text
);

insert into `conditions`
values ('student_preferences_survey_results',
        'favourite_philosopher',
        'in(twentieth_century_philosophers.name, philosophers_covered_in_the_course.name)');

insert into `conditions`
values ('student_preferences_survey_results',
        'favourite_tradition',
        'in(twentieth_century_philosophers.tradition, philosophers_covered_in_the_course.tradition)');

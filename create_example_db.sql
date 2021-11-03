drop table if exists `approved_philosophers`;

create table approved_philosophers (
    `name` text,
    `tradition` text,
    `nationality` text
);

insert into `approved_philosophers`
values ('Moritz Schlick', 'Analytic', 'German');

insert into `approved_philosophers`
values ('Rudolf Carnap', 'Analytic', 'German');

insert into `approved_philosophers`
values ('Otto Neurath', 'Analytic', 'Austrian');

insert into `approved_philosophers`
values ('Friedrich Waismann', 'Analytic', 'Austrian');

insert into `approved_philosophers`
values ('Martin Heidegger', 'Continental', 'German');

insert into `approved_philosophers`
values ('A. J. Ayer', 'Analytic', 'English');

insert into `approved_philosophers`
values ('J. P. Sartre', 'Continental', 'French');

insert into `approved_philosophers`
values ('Maurice Merleau-Ponty', 'Continental', 'French');

insert into `approved_philosophers`
values ('Judith Butler', 'Continental', 'American');

------------------------------------------

drop table if exists `student_preferences_survey_results`;

create table `student_preferences_survey_results` (
    `student_name` text,
    `favourite_philosopher` text
);

insert into `student_preferences_survey_results`
values ('Brunhilda', 'Friedrich Waismann');

insert into `student_preferences_survey_results`
values ('Fredegund', 'Judith Butler');

insert into `student_preferences_survey_results`
values ('Jonathan', 'Immanuel Kant');

insert into `student_preferences_survey_results`
values ('Harold', 'Rudolf Carnap');

------------------------------------------
drop table if exists `conditions`;

create table `conditions` (
    `table` text,
    `column` text,
    `condition` text
);

insert into `conditions`
values ('student_preferences_survey_results', 'favourite_philosopher', 'in(approved_philosophers.name)');

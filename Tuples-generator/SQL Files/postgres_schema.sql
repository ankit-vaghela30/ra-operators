CREATE TABLE student (
  id int NOT NULL,
  name char(45) DEFAULT NULL,
  address char(45) DEFAULT NULL,
  status char(45) DEFAULT NULL,
  PRIMARY KEY (id)
);

CREATE TABLE department (
  id char(45) NOT NULL,
  name char(45) DEFAULT NULL,
  PRIMARY KEY (id)
);


CREATE TABLE professor (
  id int NOT NULL,
  name char(45) DEFAULT NULL,
  deptID char(45) DEFAULT NULL,
  PRIMARY KEY (id),
  CONSTRAINT deptID FOREIGN KEY (deptID) REFERENCES department (id) ON DELETE NO ACTION ON UPDATE NO ACTION
);

CREATE TABLE course (
  crscode char(45) NOT NULL,
  deptID char(45) DEFAULT NULL,
  crsName char(45) DEFAULT NULL,
  PRIMARY KEY (crscode),
  CONSTRAINT deptID FOREIGN KEY (deptID) REFERENCES department (id) ON DELETE NO ACTION ON UPDATE NO ACTION
);


CREATE TABLE teaching (
  crsCode char(45) NOT NULL,
  semester char(45) NOT NULL,
  profId int DEFAULT NULL,
  PRIMARY KEY (crsCode,semester),
  CONSTRAINT crsCode FOREIGN KEY (crsCode) REFERENCES course (crscode) ON DELETE NO ACTION ON UPDATE NO ACTION,
  CONSTRAINT profId FOREIGN KEY (profId) REFERENCES professor (id) ON DELETE NO ACTION ON UPDATE NO ACTION
) ;

CREATE TABLE courses_student
(
  id integer,
  crscode char(45),
  PRIMARY KEY (id,crscode),
  CONSTRAINT crscode FOREIGN KEY (crscode) REFERENCES course (crscode) ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT id FOREIGN KEY (id) REFERENCES student (id) ON UPDATE NO ACTION ON DELETE NO ACTION
);



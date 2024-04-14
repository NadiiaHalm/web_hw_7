import random

from faker import Faker
from sqlalchemy.exc import SQLAlchemyError

from conf.db import session
from conf.models import Teacher, Student, Group, Grade, Subject

fake = Faker('uk-UA')


def insert_teachers():
    for _ in range(3):
        teacher = Teacher(
            fullname=fake.name()
        )
        session.add(teacher)


def insert_groups():
    for _ in range(3):
        group = Group(
            name=fake.word()
        )
        session.add(group)


def insert_subjects():
    for _ in range(5):
        subject = Subject(
            name=fake.catch_phrase(),
            teacher_id=random.randint(1, 3)
        )
        session.add(subject)


def insert_grades_and_students():
    for _ in range(30):
        student = Student(fullname=fake.name(), group_id=random.randint(1, 3))
        session.add(student)
        for subject_id in range(1, 6):
            for _ in range(random.randint(3, 5)):
                grade = Grade(grade=random.randint(1, 10),
                              grade_date=fake.date_this_decade(before_today=True),
                              student_id=random.randint(1, 30),
                              subjects_id=subject_id)
                session.add(grade)


if __name__ == '__main__':
    try:
        insert_teachers()
        insert_groups()
        insert_subjects()
        insert_grades_and_students()
        session.commit()
    except SQLAlchemyError as e:
        session.rollback()
        print(e)
    finally:
        session.close()

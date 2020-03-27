package com.thoughtworks;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class StudentRepository {

  public void save(List<Student> students) {
      students.forEach(this::save);
  }

  public void save(Student student){
    // TODO:
    String sql = "INSERT INTO student(id, name, sex, admission_year, birthday, classroom)"
            + "values(" + "?,?,?,?,?,?)";
    Connection conn;
    PreparedStatement ptmt;
    try {
      conn = DbUtil.getConnection();
      ptmt = conn.prepareStatement(sql);
      addData(ptmt, student);

      ptmt.executeLargeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public List<Student> query(){
    // TODO:
    Connection conn;
    Statement stmt;
    ResultSet rs;
    try {
      conn = DbUtil.getConnection();
      stmt = conn.createStatement();
      rs = stmt.executeQuery("SELECT id, name, sex, admission_year, birthday, classroom FROM student");

    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return addToListFromResultSet(rs);
  }

  public List<Student> queryByClassId(String classId) {
    // TODO:
    String sql = "SELECT id, name, sex, admission_year, birthday, classroom FROM student " +
            "WHERE classroom = ? ORDER BY id DESC";
    Connection conn;
    PreparedStatement ptmt;
    try {
      conn = DbUtil.getConnection();
      ptmt = conn.prepareStatement(sql);
      ptmt.setString(1, classId);
      ResultSet rs = ptmt.executeQuery();
      return addToListFromResultSet(rs);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public void update(String id, Student student) {
    // TODO:
    String sql = "UPDATE student SET id = ?,name = ?,sex = ?,admission_year = ?,birthday = ?,classroom = ? WHERE id = ?";
    Connection conn;
    PreparedStatement ptmt;
    try {
      conn = DbUtil.getConnection();
      ptmt = conn.prepareStatement(sql);
      addData(ptmt, student);
      ptmt.setString(7, id);

      ptmt.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

  }

  public void delete(String id) {
    // TODO:
    Connection conn;
    PreparedStatement ptmt;
    try {
      conn = DbUtil.getConnection();
      String sql = "DELETE FROM student WHERE id = ?";
      ptmt = conn.prepareStatement(sql);

      ptmt.setString(1, id);
      ptmt.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public List<Student> addToListFromResultSet(ResultSet rs) {
    List<Student> students = new ArrayList<>();
    Student s;
    try {
      while (rs.next()) {
        s = new Student();
        s.setId(rs.getString("id"));
        s.setName(rs.getString("name"));
        s.setGender(rs.getString("sex"));
        s.setAdmissionYear(rs.getInt("admission_year"));
        s.setBirthday(rs.getDate("birthday"));
        s.setClassId(rs.getString("classroom"));

        students.add(s);
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return students;
  }

  public void addData(PreparedStatement preparedStatement, Student student) {
    try  {
      preparedStatement.setString(1, student.getId());
      preparedStatement.setString(2, student.getName());
      preparedStatement.setString(3, student.getGender());
      preparedStatement.setInt(4, student.getAdmissionYear());
      preparedStatement.setDate(5, new Date(student.getBirthday().getTime()));
      preparedStatement.setString(6, student.getClassId());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}

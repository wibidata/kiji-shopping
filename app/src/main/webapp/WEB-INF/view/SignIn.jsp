<!--
  (c) Copyright 2013 WibiData, Inc.
 
  See the NOTICE file distributed with this work for additional
  information regarding copyright ownership.
 
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at
 
      http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/functions" prefix="fn" %>
<%@ page contentType="text/html;charset=UTF-8"%>
<!doctype html>
<html>
  <head>
    <meta http-equiv="Content-Type" content="text/html;charset=UTF-8">
    <title>KijiShopping - Sign in</title>
    <link rel="stylesheet" type="text/css" href="/bootstrap/css/bootstrap.min.css"/>
    <link rel="stylesheet" type="text/css" href="/static/css/shopping.css"/>
  </head>
  <body>
    <div class="container">
      <div class="page-header">
        <h1 class="logo-prefix"><a href="/">KijiShopping</a> <small>Sign in</small></h1>
      </div>

      <p>Please fill out the following form to sign in to your KijiShopping account.</p>

      <form method="post" class="form-horizontal">
        <fieldset>
          <legend>
            Account information
          </legend>
          <div class="control-group">
            <label class="control-label" for="login">Login</label>
            <div class="controls">
              <input type="text" class="input" id="login" name="login" value="${login}"/>
            </div>
          </div>
          <div class="control-group">
            <label class="control-label" for="password">Password</label>
            <div class="controls">
              <input type="password" class="input" id="password" name="password"
                     value="${password}"/>
            </div>
          </div>
          <c:if test="${fn:length(error) gt 0}">
            <div class="alert alert-error">${error}</div>
          </c:if>
          <div class="form-actions">
            <input type="submit" class="btn btn-primary" value="Sign in"/>
            <a href="/" class="btn">Cancel</a>
          </div>
        </fieldset>
      </form>
    </div>
  </body>
</html>

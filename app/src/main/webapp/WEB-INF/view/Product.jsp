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
    <title>KijiShopping - ${product.name}</title>
    <link rel="stylesheet" type="text/css" href="/bootstrap/css/bootstrap.min.css"/>
    <link rel="stylesheet" type="text/css" href="/static/css/shopping.css"/>
  </head>
  <body>
    <div class="container">
      <div class="page-header">
        <div class="account-info">
          <c:if test="${fn:length(login) gt 0}">
            <b>${login}</b> | <a href="/signout">Sign out</a>
          </c:if>
        </div>
        <h1 class="logo-prefix"><a href="/">KijiShopping</a> <small>powered by Kiji</small></h1>
      </div>
      <div class="hero-unit dish-large" style="background-image: url(${product.thumbnailXL})">
        <div class="well">
          <h1>${product.name} <small>${product.category}</small></h1>
          <img src="${product.thumbnail}" class="thumbnail pull-left"/>
          <p>
            ${product.description}
          </p>
          <c:if test="${fn:length(login) gt 0}">
            <p class="btn-group">
              <a class="btn btn-large
                        <c:if test="${empty currentRating || currentRating eq 1}">
                          btn-success
                        </c:if>
                        " href="/product?id=${product.id}&rate=1">Love it!</a>
              <a class="btn btn-large
                        <c:if test="${empty currentRating || currentRating eq 0}">
                          btn-warning
                        </c:if>
                        " href="/product?id=${product.id}&rate=0">It's OK</a>
              <a class="btn btn-large
                        <c:if test="${empty currentRating || currentRating eq -1}">
                          btn-danger
                        </c:if>
                        " href="/product?id=${product.id}&rate=-1">Not for me</a>
            </p>
          </c:if>
        </div>
      </div>

      <div class="well">
        <a href="/">Home</a>
      </div>
    </div>
  </body>
</html>

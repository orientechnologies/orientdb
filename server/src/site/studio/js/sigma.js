/* sigmajs.org - an open-source light-weight JavaScript graph drawing library - Version: 0.1 - Author:  Alexis Jacomy - License: MIT */
var sigma = {
    tools: {},
    classes: {},
    instances: {}
};
(function () {
    if (!Array.prototype.some) Array.prototype.some = function (i, n) {
        var g = this.length;
        if ("function" != typeof i) throw new TypeError;
        for (var j = 0; j < g; j++) if (j in this && i.call(n, this[j], j, this)) return !0;
        return !1
    };
    if (!Array.prototype.forEach) Array.prototype.forEach = function (i, n) {
        var g = this.length;
        if ("function" != typeof i) throw new TypeError;
        for (var j = 0; j < g; j++) j in this && i.call(n, this[j], j, this)
    };
    if (!Array.prototype.map) Array.prototype.map = function (i, n) {
        var g = this.length;
        if ("function" != typeof i) throw new TypeError;
        for (var j = Array(g), m = 0; m < g; m++) m in this && (j[m] = i.call(n, this[m], m, this));
        return j
    };
    if (!Array.prototype.filter) Array.prototype.filter = function (i, n) {
        var g = this.length;
        if ("function" != typeof i) throw new TypeError;
        for (var j = [], m = 0; m < g; m++) if (m in this) {
            var t = this[m];
            i.call(n, t, m, this) && j.push(t)
        }
        return j
    };
    if (!Object.keys) Object.keys = function () {
        var i = Object.prototype.hasOwnProperty,
            n = !{
                toString: null
            }.propertyIsEnumerable("toString"),
            g = "toString,toLocaleString,valueOf,hasOwnProperty,isPrototypeOf,propertyIsEnumerable,constructor".split(","),
            j = g.length;
        return function (m) {
            if ("object" !== typeof m && "function" !== typeof m || null === m) throw new TypeError("Object.keys called on non-object");
            var t = [],
                p;
            for (p in m) i.call(m, p) && t.push(p);
            if (n) for (p = 0; p < j; p++) i.call(m, g[p]) && t.push(g[p]);
            return t
        }
    }()
})();
sigma.classes.Cascade = function () {
    this.p = {};
    this.config = function (i, n) {
        if ("string" == typeof i && void 0 == n) return this.p[i];
        var g = "object" == typeof i && void 0 == n ? i : {};
        "string" == typeof i && (g[i] = n);
        for (var j in g) void 0 != this.p[j] && (this.p[j] = g[j]);
        return this
    }
};
sigma.classes.EventDispatcher = function () {
    var i = {},
        n = this;
    this.one = function (g, j) {
        if (!j || !g) return n;
        ("string" == typeof g ? g.split(" ") : g).forEach(function (g) {
            i[g] || (i[g] = []);
            i[g].push({
                h: j,
                one: !0
            })
        });
        return n
    };
    this.bind = function (g, j) {
        if (!j || !g) return n;
        ("string" == typeof g ? g.split(" ") : g).forEach(function (g) {
            i[g] || (i[g] = []);
            i[g].push({
                h: j,
                one: !1
            })
        });
        return n
    };
    this.unbind = function (g, j) {
        g || (i = {});
        var m = "string" == typeof g ? g.split(" ") : g;
        j ? m.forEach(function (g) {
            i[g] && (i[g] = i[g].filter(function (g) {
                return g.h != j
            }));
            i[g] && 0 == i[g].length && delete i[g]
        }) : m.forEach(function (g) {
            delete i[g]
        });
        return n
    };
    this.dispatch = function (g, j) {
        i[g] && (i[g].forEach(function (i) {
            i.h({
                type: g,
                content: j,
                target: n
            })
        }), i[g] = i[g].filter(function (g) {
            return !g.one
        }));
        return n
    }
};
(function () {
    var i;

    function n() {
        function b(a) {
        	if( !a )
        		return null;
        	
            return {
                x: a.x,
                y: a.y,
                size: a.size,
                degree: a.degree,
                displayX: a.displayX,
                displayY: a.displayY,
                displaySize: a.displaySize,
                label: a.label,
                id: a.id,
                color: a.color,
                fixed: a.fixed,
                active: a.active,
                hidden: a.hidden,
                attr: a.attr
            }
        }
        function h(a) {
        	if( !a )
        		return null;
        	
            return {
                source: a.source.id,
                target: a.target.id,
                size: a.size,
                type: a.type,
                weight: a.weight,
                displaySize: a.displaySize,
                label: a.label,
                id: a.id,
                attr: a.attr,
                color: a.color
            }
        }
        function e() {
            d.nodes = [];
            d.nodesIndex = {};
            d.edges = [];
            d.edgesIndex = {};
            return d
        }
        sigma.classes.Cascade.call(this);
        sigma.classes.EventDispatcher.call(this);
        var d = this;
        this.p = {
            minNodeSize: 0,
            maxNodeSize: 0,
            minEdgeSize: 0,
            maxEdgeSize: 0,
            scalingMode: "inside",
            nodesPowRatio: 0.5,
            edgesPowRatio: 0
        };
        this.borders = {};
        e();
        this.addNode = function (a, b) {
            if (d.nodesIndex[a]) throw Error('Node "' + a + '" already exists.');
            var b = b || {},
                c = {
                    x: 0,
                    y: 0,
                    size: 1,
                    degree: 0,
                    fixed: !1,
                    active: !1,
                    hidden: !1,
                    label: a.toString(),
                    id: a.toString(),
                    attr: {}
                },
                e;
            for (e in b) switch (e) {
            case "id":
                break;
            case "x":
            case "y":
            case "size":
                c[e] = +b[e];
                break;
            case "fixed":
            case "active":
            case "hidden":
                c[e] = !! b[e];
                break;
            case "color":
            case "label":
                c[e] = b[e];
                break;
            default:
                c.attr[e] = b[e]
            }
            d.nodes.push(c);
            d.nodesIndex[a.toString()] = c;
            return d
        };
        this.addEdge = function (a, b, c, e) {
            if (d.edgesIndex[a]) throw Error('Edge "' + a + '" already exists.');
            if (!d.nodesIndex[b]) throw Error("Edge's source \"" + b + '" does not exist yet.');
            if (!d.nodesIndex[c]) throw Error("Edge's target \"" + c + '" does not exist yet.');
            e = e || {};
            b = {
                source: d.nodesIndex[b],
                target: d.nodesIndex[c],
                size: 1,
                weight: 1,
                displaySize: 0.5,
                label: a.toString(),
                id: a.toString(),
                attr: {}
            };
            b.source.degree++;
            b.target.degree++;
            for (var k in e) switch (k) {
            case "id":
            case "source":
            case "target":
                break;
            case "size":
                b[k] = +e[k];
                break;
            case "color":
                b[k] = e[k].toString();
                break;
            case "type":
                b[k] = e[k].toString();
                break;
            case "label":
                b[k] = e[k];
                break;
            default:
                b.attr[k] = e[k]
            }
            d.edges.push(b);
            d.edgesIndex[a.toString()] = b;
            return d
        };
        this.dropNode = function (a) {
            ((a instanceof Array ? a : [a]) || []).forEach(function (a) {
                if (d.nodesIndex[a]) {
                    var c = null;
                    d.nodes.some(function (b, d) {
                        return b.id == a ? (c = d, !0) : !1
                    });
                    null != c && d.nodes.splice(c, 1);
                    delete d.nodesIndex[a];
                    d.edges = d.edges.filter(function (c) {
                        return c.source.id == a || c.target.id == a ? (delete d.edgesIndex[c.id], !1) : !0
                    })
                } else sigma.log('Node "' + a + '" does not exist.')
            });
            return d
        };
        this.dropEdge = function (a) {
            ((a instanceof Array ? a : [a]) || []).forEach(function (a) {
                if (d.edgesIndex[a]) {
                    var c = null;
                    d.edges.some(function (b, d) {
                        return b.id == a ? (c = d, !0) : !1
                    });
                    null != c && d.edges.splice(c, 1);
                    delete d.edgesIndex[a]
                } else sigma.log('Edge "' + a + '" does not exist.')
            });
            return d
        };
        this.iterEdges = function (a, b) {
            var c = b ? b.map(function (a) {
                return d.edgesIndex[a]
            }) : d.edges,
                e = c.map(h);
            e.forEach(a);
            c.forEach(function (a, c) {
                var b = e[c],
                    l;
                for (l in b) switch (l) {
                case "id":
                case "weight":
                case "displaySize":
                    break;
                case "size":
                    a[l] = +b[l];
                    break;
                case "source":
                case "target":
                    a[l] = d.nodesIndex[l] || a[l];
                    break;
                case "color":
                case "label":
                case "type":
                    a[l] = (b[l] || "").toString();
                    break;
                default:
                    a.attr[l] = b[l]
                }
            });
            return d
        };
        this.iterNodes = function (a, e) {
            var c = e ? e.map(function (a) {
                return d.nodesIndex[a]
            }) : d.nodes,
                h = c.map(b);
            h.forEach(a);
            c.forEach(function (a, c) {
                var b = h[c],
                    d;
                for (d in b) switch (d) {
                case "id":
                case "attr":
                case "degree":
                case "displayX":
                case "displayY":
                case "displaySize":
                    break;
                case "x":
                case "y":
                case "size":
                    a[d] = +b[d];
                    break;
                case "fixed":
                case "active":
                case "hidden":
                    a[d] = !! b[d];
                    break;
                case "color":
                case "label":
                    a[d] = b[d].toString();
                    break;
                default:
                    a.attr[d] = b[d]
                }
            });
            return d
        };
        this.getEdges = function (a) {
            var b = ((a instanceof Array ? a : [a]) || []).map(function (a) {
                return h(d.edgesIndex[a])
            });
            return a instanceof
            Array ? b : b[0]
        };
        this.getNodes = function (a) {
            var e = ((a instanceof Array ? a : [a]) || []).map(function (a) {
                return b(d.nodesIndex[a])
            });
            return a instanceof Array ? e : e[0]
        };
        this.empty = e;
        this.rescale = function (a, b, c, e) {
            var k = 0,
                f = 0;
            c && d.nodes.forEach(function (a) {
                f = Math.max(a.size, f)
            });
            e && d.edges.forEach(function (a) {
                k = Math.max(a.size, k)
            });
            var f = f || 1,
                k = k || 1,
                h, g, s, q;
            c && d.nodes.forEach(function (a) {
                g = Math.max(a.x, g || a.x);
                h = Math.min(a.x, h || a.x);
                q = Math.max(a.y, q || a.y);
                s = Math.min(a.y, s || a.y)
            });
            var z = "outside" == d.p.scalingMode ? Math.max(a / Math.max(g - h, 1), b / Math.max(q - s, 1)) : Math.min(a / Math.max(g - h, 1), b / Math.max(q - s, 1)),
                i, j;
            !d.p.maxNodeSize && !d.p.minNodeSize ? (i = 1, j = 0) : d.p.maxNodeSize == d.p.minNodeSize ? (i = 0, j = d.p.maxNodeSize) : (i = (d.p.maxNodeSize - d.p.minNodeSize) / f, j = d.p.minNodeSize);
            var m, w;
            !d.p.maxEdgeSize && !d.p.minEdgeSize ? (m = 1, w = 0) : (m = d.p.maxEdgeSize == d.p.minEdgeSize ? 0 : (d.p.maxEdgeSize - d.p.minEdgeSize) / k, w = d.p.minEdgeSize);
            c && d.nodes.forEach(function (c) {
                c.displaySize = c.size * i + j;
                if (!c.fixed) c.displayX = (c.x - (g + h) / 2) * z + a / 2, c.displayY = (c.y - (q + s) / 2) * z + b / 2
            });
            e && d.edges.forEach(function (a) {
                a.displaySize = a.size * m + w
            });
            return d
        };
        this.translate = function (a, b, c, e, k) {
            var f = Math.pow(c, d.p.nodesPowRatio);
            e && d.nodes.forEach(function (d) {
                if (!d.fixed) d.displayX = d.displayX * c + a, d.displayY = d.displayY * c + b;
                d.displaySize *= f
            });
            f = Math.pow(c, d.p.edgesPowRatio);
            k && d.edges.forEach(function (a) {
                a.displaySize *= f
            });
            return d
        };
        this.setBorders = function () {
            d.borders = {};
            d.nodes.forEach(function (a) {
                d.borders.minX = Math.min(void 0 == d.borders.minX ? a.displayX - a.displaySize : d.borders.minX, a.displayX - a.displaySize);
                d.borders.maxX = Math.max(void 0 == d.borders.maxX ? a.displayX + a.displaySize : d.borders.maxX, a.displayX + a.displaySize);
                d.borders.minY = Math.min(void 0 == d.borders.minY ? a.displayY - a.displaySize : d.borders.minY, a.displayY - a.displaySize);
                d.borders.maxY = Math.max(void 0 == d.borders.maxY ? a.displayY - a.displaySize : d.borders.maxY, a.displayY - a.displaySize)
            })
        };
        this.checkHover = function (a, b) {
            var c, e, k, f = [],
                h = [];
            d.nodes.forEach(function (d) {
                if (d.hidden) d.hover = !1;
                else {
                    c = Math.abs(d.displayX - a);
                    e = Math.abs(d.displayY - b);
                    k = d.displaySize;
                    var s = d.hover,
                        q = c < k && e < k && Math.sqrt(c * c + e * e) < k;
                    if (s && !q) d.hover = !1, h.push(d.id);
                    else if (q && !s) d.hover = !0, f.push(d.id)
                }
            });
            f.length && d.dispatch("overnodes", f);
            h.length && d.dispatch("outnodes", h);
            return d
        }
    }
    function g(b, h) {
        function e() {
            var a;
            a = "<p>GLOBAL :</p>";
            for (var b in d.p.globalProbes) a += "<p>" + b + " : " + d.p.globalProbes[b]() + "</p>";
            a += "<br><p>LOCAL :</p>";
            for (b in d.p.localProbes) a += "<p>" + b + " : " + d.p.localProbes[b]() + "</p>";
            d.p.dom.innerHTML = a;
            return d
        }
        sigma.classes.Cascade.call(this);
        var d = this;
        this.instance = b;
        this.monitoring = !1;
        this.p = {
            fps: 40,
            dom: h,
            globalProbes: {
                "Time (ms)": sigma.chronos.getExecutionTime,
                Queue: sigma.chronos.getQueuedTasksCount,
                Tasks: sigma.chronos.getTasksCount,
                FPS: sigma.chronos.getFPS
            },
            localProbes: {
                "Nodes count": function () {
                    return d.instance.graph.nodes.length
                },
                "Edges count": function () {
                    return d.instance.graph.edges.length
                }
            }
        };
        this.activate = function () {
            if (!d.monitoring) d.monitoring = window.setInterval(e, 1E3 / d.p.fps);
            return d
        };
        this.desactivate = function () {
            if (d.monitoring) window.clearInterval(d.monitoring), d.monitoring = null, d.p.dom.innerHTML = "";
            return d
        }
    }
    function j(b) {
        function h(b) {
            if (a.p.mouseEnabled && (e(a.mouseX, a.mouseY, a.ratio * (0 < (void 0 != b.wheelDelta && b.wheelDelta || void 0 != b.detail && -b.detail) ? a.p.zoomMultiply : 1 / a.p.zoomMultiply)), a.p.blockScroll)) b.preventDefault ? b.preventDefault() : b.returnValue = !1
        }
        function e(b, c, e) {
            if (!a.isMouseDown && (window.clearInterval(a.interpolationID), n = void 0 != e, i = a.stageX, j = b, k = a.stageY, l = c, f = e || a.ratio, f = Math.min(Math.max(f, a.p.minRatio), a.p.maxRatio), u = a.p.directZooming ? 1 - (n ? a.p.zoomDelta : a.p.dragDelta) : 0, a.ratio != f || a.stageX != j || a.stageY != l)) d(), a.interpolationID = window.setInterval(d, 50), a.dispatch("startinterpolate")
        }
        function d() {
            u += n ? a.p.zoomDelta : a.p.dragDelta;
            u = Math.min(u, 1);
            var b = sigma.easing.quadratic.easeout(u),
                c = a.ratio;
            a.ratio = c * (1 - b) + f * b;
            n ? (a.stageX = j + (a.stageX - j) * a.ratio / c, a.stageY = l + (a.stageY - l) * a.ratio / c) : (a.stageX = i * (1 - b) + j * b, a.stageY = k * (1 - b) + l * b);
            a.dispatch("interpolate");
            if (1 <= u) window.clearInterval(a.interpolationID), b = a.ratio, n ? (a.ratio = f, a.stageX = j + (a.stageX - j) * a.ratio / b, a.stageY = l + (a.stageY - l) * a.ratio / b) : (a.stageX = j, a.stageY = l), a.dispatch("stopinterpolate")
        }
        sigma.classes.Cascade.call(this);
        sigma.classes.EventDispatcher.call(this);
        var a = this;
        this.p = {
            minRatio: 1,
            maxRatio: 32,
            marginRatio: 1,
            zoomDelta: 0.1,
            dragDelta: 0.3,
            zoomMultiply: 2,
            directZooming: !1,
            blockScroll: !0,
            inertia: 1.1,
            mouseEnabled: !0
        };
        var g = 0,
            c = 0,
            i = 0,
            k = 0,
            f = 1,
            j = 0,
            l = 0,
            s = 0,
            q = 0,
            z = 0,
            m = 0,
            u = 0,
            n = !1;
        this.stageY = this.stageX = 0;
        this.ratio = 1;
        this.mouseY = this.mouseX = 0;
        this.isMouseDown = !1;
        b.addEventListener("DOMMouseScroll", h, !0);
        b.addEventListener("mousewheel", h, !0);
        b.addEventListener("mousemove", function (b) {
            a.mouseX = void 0 != b.offsetX && b.offsetX || void 0 != b.layerX && b.layerX || void 0 != b.clientX && b.clientX;
            a.mouseY = void 0 != b.offsetY && b.offsetY || void 0 != b.layerY && b.layerY || void 0 != b.clientY && b.clientY;
            if (a.isMouseDown) {
                var d = a.mouseX - g + i,
                    f = a.mouseY - c + k;
                if (d != a.stageX || f != a.stageY) q = s, m = z, s = d, z = f, a.stageX = d, a.stageY = f, a.dispatch("drag")
            }
            a.dispatch("move");
            b.preventDefault ? b.preventDefault() : b.returnValue = !1
        }, !0);
        b.addEventListener("mousedown", function (b) {
            if (a.p.mouseEnabled) a.isMouseDown = !0, a.dispatch("mousedown"), i = a.stageX, k = a.stageY, g = a.mouseX, c = a.mouseY, q = s = a.stageX, m = z = a.stageY, a.dispatch("startdrag"), b.preventDefault ? b.preventDefault() : b.returnValue = !1
        }, !0);
        document.addEventListener("mouseup", function (b) {
            if (a.p.mouseEnabled && a.isMouseDown) a.isMouseDown = !1, a.dispatch("mouseup"), (i != a.stageX || k != a.stageY) && e(a.stageX + a.p.inertia * (a.stageX - q), a.stageY + a.p.inertia * (a.stageY - m)), b.preventDefault ? b.preventDefault() : b.returnValue = !1
        }, !0);
        this.checkBorders = function () {
            return a
        };
        this.interpolate = e
    }
    function m(b, h, e, d, a, g, c) {
        function i(a) {
            var b = d,
                c = "fixed" == f.p.labelSize ? f.p.defaultLabelSize : f.p.labelSizeRatio * a.displaySize;
            b.font = (f.p.hoverFontStyle || f.p.fontStyle || "") + " " + c + "px " + (f.p.hoverFont || f.p.font || "");
            b.fillStyle = "node" == f.p.labelHoverBGColor ? a.color || f.p.defaultNodeColor : f.p.defaultHoverLabelBGColor;
            b.beginPath();
            if (f.p.labelHoverShadow) b.shadowOffsetX = 0, b.shadowOffsetY = 0, b.shadowBlur = 4, b.shadowColor = f.p.labelHoverShadowColor;
            sigma.tools.drawRoundRect(b, Math.round(a.displayX - c / 2 - 2), Math.round(a.displayY - c / 2 - 2), Math.round(b.measureText(a.label).width + 1.5 * a.displaySize + c / 2 + 4), Math.round(c + 4), Math.round(c / 2 + 2), "left");
            b.closePath();
            b.fill();
            b.shadowOffsetX = 0;
            b.shadowOffsetY = 0;
            b.shadowBlur = 0;
            b.beginPath();
            b.fillStyle = "node" == f.p.nodeBorderColor ? a.color || f.p.defaultNodeColor : f.p.defaultNodeBorderColor;
            b.arc(Math.round(a.displayX), Math.round(a.displayY), a.displaySize + f.p.borderSize, 0, 2 * Math.PI, !0);
            b.closePath();
            b.fill();
            b.beginPath();
            b.fillStyle = "node" == f.p.nodeHoverColor ? a.color || f.p.defaultNodeColor : f.p.defaultNodeHoverColor;
            b.arc(Math.round(a.displayX), Math.round(a.displayY), a.displaySize, 0, 2 * Math.PI, !0);
            b.closePath();
            b.fill();
            b.fillStyle = "node" == f.p.labelHoverColor ? a.color || f.p.defaultNodeColor : f.p.defaultLabelHoverColor;
            b.fillText(a.label, Math.round(a.displayX + 1.5 * a.displaySize), Math.round(a.displayY + c / 2 - 3));
            return f
        }
        function k(a) {
            if (isNaN(a.x) || isNaN(a.y)) throw Error("A node's coordinate is not a number (id: " + a.id + ")");
            return !a.hidden && a.displayX + a.displaySize > -j / 3 && a.displayX - a.displaySize < 4 * j / 3 && a.displayY + a.displaySize > -l / 3 && a.displayY - a.displaySize < 4 * l / 3
        }
        sigma.classes.Cascade.call(this);
        var f = this;
        this.p = {
            labelColor: "default",
            defaultLabelColor: "#000",
            labelHoverBGColor: "default",
            defaultHoverLabelBGColor: "#fff",
            labelHoverShadow: !0,
            labelHoverShadowColor: "#000",
            labelHoverColor: "default",
            defaultLabelHoverColor: "#000",
            labelActiveBGColor: "default",
            defaultActiveLabelBGColor: "#fff",
            labelActiveShadow: !0,
            labelActiveShadowColor: "#000",
            labelActiveColor: "default",
            defaultLabelActiveColor: "#000",
            labelSize: "fixed",
            defaultLabelSize: 12,
            labelSizeRatio: 2,
            labelThreshold: 6,
            font: "Arial",
            hoverFont: "",
            activeFont: "",
            fontStyle: "",
            hoverFontStyle: "",
            activeFontStyle: "",
            edgeColor: "source",
            defaultEdgeColor: "#aaa",
            defaultEdgeType: "line",
            defaultNodeColor: "#aaa",
            nodeHoverColor: "node",
            defaultNodeHoverColor: "#fff",
            nodeActiveColor: "node",
            defaultNodeActiveColor: "#fff",
            borderSize: 0,
            nodeBorderColor: "node",
            defaultNodeBorderColor: "#fff",
            edgesSpeed: 200,
            nodesSpeed: 200,
            labelsSpeed: 200
        };
        var j = g,
            l = c;
        this.currentLabelIndex = this.currentNodeIndex = this.currentEdgeIndex = 0;
        this.task_drawLabel = function () {
            for (var b = a.nodes.length, c = 0; c++ < f.p.labelsSpeed && f.currentLabelIndex < b;) if (f.isOnScreen(a.nodes[f.currentLabelIndex])) {
                var d = a.nodes[f.currentLabelIndex++],
                    h = e;
                if (d.displaySize >= f.p.labelThreshold) {
                    var k = "fixed" == f.p.labelSize ? f.p.defaultLabelSize : f.p.labelSizeRatio * d.displaySize;
                    h.font = f.p.fontStyle + k + "px " + f.p.font;
                    h.fillStyle = "node" == f.p.labelColor ? d.color || f.p.defaultNodeColor : f.p.defaultLabelColor;
                    h.fillText(d.label, Math.round(d.displayX + 1.5 * d.displaySize), Math.round(d.displayY + k / 2 - 3))
                }
            } else f.currentLabelIndex++;
            return f.currentLabelIndex < b
        };
        this.task_drawEdge = function () {
            for (var b = a.edges.length, c, d, e = 0; e++ < f.p.edgesSpeed && f.currentEdgeIndex < b;) if (c = a.edges[f.currentEdgeIndex].source, d = a.edges[f.currentEdgeIndex].target, c.hidden || d.hidden || !f.isOnScreen(c) && !f.isOnScreen(d)) f.currentEdgeIndex++;
            else {
                c = a.edges[f.currentEdgeIndex++];
                d = c.source.displayX;
                var k = c.source.displayY,
                    g = c.target.displayX,
                    i = c.target.displayY,
                    j = c.color;
                if (!j) switch (f.p.edgeColor) {
                case "source":
                    j = c.source.color || f.p.defaultNodeColor;
                    break;
                case "target":
                    j = c.target.color || f.p.defaultNodeColor;
                    break;
                default:
                    j = f.p.defaultEdgeColor
                }
                var l = h;
                switch (c.type || f.p.defaultEdgeType) {
                case "curve":
                    l.strokeStyle = j;
                    l.lineWidth = c.displaySize / 3;
                    l.beginPath();
                    l.moveTo(d, k);
                    l.quadraticCurveTo((d + g) / 2 + (i - k) / 4, (k + i) / 2 + (d - g) / 4, g, i);
                    l.stroke();
                    break;
                default:
                    l.strokeStyle = j, l.lineWidth = c.displaySize / 3, l.beginPath(), l.moveTo(d, k), l.lineTo(g, i), l.stroke()
                }
            }
            return f.currentEdgeIndex < b
        };
        this.task_drawNode = function () {
            for (var c = a.nodes.length, d = 0; d++ < f.p.nodesSpeed && f.currentNodeIndex < c;) if (f.isOnScreen(a.nodes[f.currentNodeIndex])) {
                var e = a.nodes[f.currentNodeIndex++],
                    k = Math.round(10 * e.displaySize) / 10,
                    h = b;
                h.fillStyle = e.color;
                h.beginPath();
                h.arc(e.displayX, e.displayY, k, 0, 2 * Math.PI, !0);
                h.closePath();
                h.fill();
                e.hover && i(e)
            } else f.currentNodeIndex++;
            return f.currentNodeIndex < c
        };
        this.drawActiveNode = function (a) {
            var b = d;
            if (!k(a)) return f;
            var c = "fixed" == f.p.labelSize ? f.p.defaultLabelSize : f.p.labelSizeRatio * a.displaySize;
            b.font = (f.p.activeFontStyle || f.p.fontStyle || "") + " " + c + "px " + (f.p.activeFont || f.p.font || "");
            b.fillStyle = "node" == f.p.labelHoverBGColor ? a.color || f.p.defaultNodeColor : f.p.defaultActiveLabelBGColor;
            b.beginPath();
            if (f.p.labelActiveShadow) b.shadowOffsetX = 0, b.shadowOffsetY = 0, b.shadowBlur = 4, b.shadowColor = f.p.labelActiveShadowColor;
            sigma.tools.drawRoundRect(b, Math.round(a.displayX - c / 2 - 2), Math.round(a.displayY - c / 2 - 2), Math.round(b.measureText(a.label).width + 1.5 * a.displaySize + c / 2 + 4), Math.round(c + 4), Math.round(c / 2 + 2), "left");
            b.closePath();
            b.fill();
            b.shadowOffsetX = 0;
            b.shadowOffsetY = 0;
            b.shadowBlur = 0;
            b.beginPath();
            b.fillStyle = "node" == f.p.nodeBorderColor ? a.color || f.p.defaultNodeColor : f.p.defaultNodeBorderColor;
            b.arc(Math.round(a.displayX), Math.round(a.displayY), a.displaySize + f.p.borderSize, 0, 2 * Math.PI, !0);
            b.closePath();
            b.fill();
            b.beginPath();
            b.fillStyle = "node" == f.p.nodeActiveColor ? a.color || f.p.defaultNodeColor : f.p.defaultNodeActiveColor;
            b.arc(Math.round(a.displayX), Math.round(a.displayY), a.displaySize, 0, 2 * Math.PI, !0);
            b.closePath();
            b.fill();
            b.fillStyle = "node" == f.p.labelActiveColor ? a.color || f.p.defaultNodeColor : f.p.defaultLabelActiveColor;
            b.fillText(a.label, Math.round(a.displayX + 1.5 * a.displaySize), Math.round(a.displayY + c / 2 - 3));
            return f
        };
        this.drawHoverNode = i;
        this.isOnScreen = k;
        this.resize = function (a, b) {
            j = a;
            l = b;
            return f
        }
    }
    function t(b, h) {
        function e() {
            sigma.chronos.removeTask("node_" + c.id, 2).removeTask("edge_" + c.id, 2).removeTask("label_" + c.id, 2).stopTasks();
            return c
        }
        function d(a, b) {
            c.domElements[a] = document.createElement(b);
            c.domElements[a].style.position = "absolute";
            c.domElements[a].setAttribute("id", "sigma_" + a + "_" + c.id);
            c.domElements[a].setAttribute("class", "sigma_" + a + "_" + b);
            c.domElements[a].setAttribute("width", c.width + "px");
            c.domElements[a].setAttribute("height", c.height + "px");
            c.domRoot.appendChild(c.domElements[a]);
            return c
        }
        function a() {
            c.p.drawHoverNodes && (c.graph.checkHover(c.mousecaptor.mouseX, c.mousecaptor.mouseY), c.graph.nodes.forEach(function (a) {
                a.hover && !a.active && c.plotter.drawHoverNode(a)
            }));
            return c
        }
        function A() {
            c.p.drawActiveNodes && c.graph.nodes.forEach(function (a) {
                a.active && c.plotter.drawActiveNode(a)
            });
            return c
        }
        sigma.classes.Cascade.call(this);
        sigma.classes.EventDispatcher.call(this);
        var c = this;
        this.id = h.toString();
        this.p = {
            auto: !0,
            drawNodes: 2,
            drawEdges: 1,
            drawLabels: 2,
            lastNodes: 2,
            lastEdges: 0,
            lastLabels: 2,
            drawHoverNodes: !0,
            drawActiveNodes: !0
        };
        this.domRoot = b;
        this.width = this.domRoot.offsetWidth;
        this.height = this.domRoot.offsetHeight;
        this.graph = new n;
        this.domElements = {};
        d("edges", "canvas");
        d("nodes", "canvas");
        d("labels", "canvas");
        d("hover", "canvas");
        d("monitor", "div");
        d("mouse", "canvas");
        this.plotter = new m(this.domElements.nodes.getContext("2d"), this.domElements.edges.getContext("2d"), this.domElements.labels.getContext("2d"), this.domElements.hover.getContext("2d"), this.graph, this.width, this.height);
        this.monitor = new g(this, this.domElements.monitor);
        this.mousecaptor = new j(this.domElements.mouse, this.id);
        this.mousecaptor.bind("drag interpolate", function () {
            c.draw(c.p.auto ? 2 : c.p.drawNodes, c.p.auto ? 0 : c.p.drawEdges, c.p.auto ? 2 : c.p.drawLabels, !0)
        }).bind("stopdrag stopinterpolate", function () {
            c.draw(c.p.auto ? 2 : c.p.drawNodes, c.p.auto ? 1 : c.p.drawEdges, c.p.auto ? 2 : c.p.drawLabels, !0)
        }).bind("mousedown mouseup", function (a) {
            var b = c.graph.nodes.filter(function (a) {
                return !!a.hover
            }).map(function (a) {
                return a.id
            });
            c.dispatch("mousedown" == a.type ? "downgraph" : "upgraph");
            b.length && c.dispatch("mousedown" == a.type ? "downnodes" : "upnodes", b)
        }).bind("move", function () {
            c.domElements.hover.getContext("2d").clearRect(0, 0, c.domElements.hover.width, c.domElements.hover.height);
            a();
            A()
        });
        sigma.chronos.bind("startgenerators", function () {
            sigma.chronos.getGeneratorsIDs().some(function (a) {
                return !!a.match(RegExp("_ext_" + c.id + "$", ""))
            }) && c.draw(c.p.auto ? 2 : c.p.drawNodes, c.p.auto ? 0 : c.p.drawEdges, c.p.auto ? 2 : c.p.drawLabels)
        }).bind("stopgenerators", function () {
            c.draw()
        });
        for (var B = 0; B < i.length; B++) i[B](this);
        this.draw = function (a, b, d, h) {
            if (h && sigma.chronos.getGeneratorsIDs().some(function (a) {
                return !!a.match(RegExp("_ext_" + c.id + "$", ""))
            })) return c;
            a = void 0 == a ? c.p.drawNodes : a;
            b = void 0 == b ? c.p.drawEdges : b;
            d = void 0 == d ? c.p.drawLabels : d;
            h = {
                nodes: a,
                edges: b,
                labels: d
            };
            c.p.lastNodes = a;
            c.p.lastEdges = b;
            c.p.lastLabels = d;
            e();
            c.graph.rescale(c.width, c.height, 0 < a, 0 < b).setBorders();
            c.mousecaptor.checkBorders(c.graph.borders, c.width, c.height);
            c.graph.translate(c.mousecaptor.stageX, c.mousecaptor.stageY, c.mousecaptor.ratio, 0 < a, 0 < b);
            c.dispatch("graphscaled");
            for (var g in c.domElements) "canvas" == c.domElements[g].nodeName.toLowerCase() && (void 0 == h[g] || 0 <= h[g]) && c.domElements[g].getContext("2d").clearRect(0, 0, c.domElements[g].width, c.domElements[g].height);
            c.plotter.currentEdgeIndex = 0;
            c.plotter.currentNodeIndex = 0;
            c.plotter.currentLabelIndex = 0;
            g = null;
            h = !1;
            if (a) if (1 < a) for (; c.plotter.task_drawNode(););
            else sigma.chronos.addTask(c.plotter.task_drawNode, "node_" + c.id, !1), h = !0, g = "node_" + c.id;
            if (d) if (1 < d) for (; c.plotter.task_drawLabel(););
            else g ? sigma.chronos.queueTask(c.plotter.task_drawLabel, "label_" + c.id, g) : sigma.chronos.addTask(c.plotter.task_drawLabel, "label_" + c.id, !1), h = !0, g = "label_" + c.id;
            if (b) if (1 < b) for (; c.plotter.task_drawEdge(););
            else g ? sigma.chronos.queueTask(c.plotter.task_drawEdge, "edge_" + c.id, g) : sigma.chronos.addTask(c.plotter.task_drawEdge, "edge_" + c.id, !1), h = !0, g = "edge_" + c.id;
            c.dispatch("draw");
            c.refresh();
            h && sigma.chronos.runTasks();
            return c
        };
        this.resize = function (a, b) {
            var d = c.width,
                e = c.height;
            void 0 != a && void 0 != b ? (c.width = a, c.height = b) : (c.width = c.domRoot.offsetWidth, c.height = c.domRoot.offsetHeight);
            if (d != c.width || e != c.height) {
                for (var h in c.domElements) c.domElements[h].setAttribute("width", c.width + "px"), c.domElements[h].setAttribute("height", c.height + "px");
                c.plotter.resize(c.width, c.height);
                c.draw(c.p.lastNodes, c.p.lastEdges, c.p.lastLabels, !0)
            }
            return c
        };
        this.refresh = function () {
            c.domElements.hover.getContext("2d").clearRect(0, 0, c.domElements.hover.width, c.domElements.hover.height);
            a();
            A();
            return c
        };
        this.drawHover = a;
        this.drawActive = A;
        this.clearSchedule = e;
        window.addEventListener("resize", function () {
            c.resize()
        })
    }
    function p(b) {
        var h = this;
        sigma.classes.EventDispatcher.call(this);
        this._core = b;
        this.kill = function () {};
        this.getID = function () {
            return b.id
        };
        this.configProperties = function (e, d) {
            var a = b.config(e, d);
            return a == b ? h : a
        };
        this.drawingProperties = function (e, d) {
            var a = b.plotter.config(e, d);
            return a == b.plotter ? h : a
        };
        this.mouseProperties = function (e, d) {
            var a = b.mousecaptor.config(e, d);
            return a == b.mousecaptor ? h : a
        };
        this.graphProperties = function (e, d) {
            var a = b.graph.config(e, d);
            return a == b.graph ? h : a
        };
        this.getMouse = function () {
            return {
                mouseX: b.mousecaptor.mouseX,
                mouseY: b.mousecaptor.mouseY,
                down: b.mousecaptor.isMouseDown
            }
        };
        this.position = function (e, d, a) {
            if (0 == arguments.length) return {
                stageX: b.mousecaptor.stageX,
                stageY: b.mousecaptor.stageY,
                ratio: b.mousecaptor.ratio
            };
            b.mousecaptor.stageX = void 0 != e ? e : b.mousecaptor.stageX;
            b.mousecaptor.stageY = void 0 != d ? d : b.mousecaptor.stageY;
            b.mousecaptor.ratio = void 0 != a ? a : b.mousecaptor.ratio;
            return h
        };
        this.goTo = function (e, d, a) {
            b.mousecaptor.interpolate(e, d, a);
            return h
        };
        this.zoomTo = function (e, d, a) {
            a = Math.min(Math.max(b.mousecaptor.config("minRatio"), a), b.mousecaptor.config("maxRatio"));
            a == b.mousecaptor.ratio ? b.mousecaptor.interpolate(e - b.width / 2 + b.mousecaptor.stageX, d - b.height / 2 + b.mousecaptor.stageY) : b.mousecaptor.interpolate((a * e - b.mousecaptor.ratio * b.width / 2) / (a - b.mousecaptor.ratio), (a * d - b.mousecaptor.ratio * b.height / 2) / (a - b.mousecaptor.ratio), a);
            return h
        };
        this.resize = function (e, d) {
            b.resize(e, d);
            return h
        };
        this.draw = function (e, d, a, g) {
            b.draw(e, d, a, g);
            return h
        };
        this.refresh = function () {
            b.refresh();
            return h
        };
        this.addGenerator = function (e, d, a) {
            sigma.chronos.addGenerator(e + "_ext_" + b.id, d, a);
            return h
        };
        this.removeGenerator = function (e) {
            sigma.chronos.removeGenerator(e + "_ext_" + b.id);
            return h
        };
        this.addNode = function (e, d) {
            b.graph.addNode(e, d);
            return h
        };
        this.addEdge = function (e, d, a, g) {
            b.graph.addEdge(e, d, a, g);
            return h
        };
        this.dropNode = function (e) {
            b.graph.dropNode(e);
            return h
        };
        this.dropEdge = function (e) {
            b.graph.dropEdge(e);
            return h
        };
        this.pushGraph = function (e, d) {
            e.nodes && e.nodes.forEach(function (a) {
                a.id && (!d || !b.graph.nodesIndex[a.id]) && h.addNode(a.id, a)
            });
            e.edges && e.edges.forEach(function (a) {
                (validID = a.source && a.target && a.id) && (!d || !b.graph.edgesIndex[a.id]) && h.addNode(a.id, a.source, a.target, a)
            });
            return h
        };
        this.emptyGraph = function () {
            b.graph.empty();
            return h
        };
        this.getNodesCount = function () {
            return b.graph.nodes.length
        };
        this.getEdgesCount = function () {
            return b.graph.edges.length
        };
        this.iterNodes = function (e, d) {
            b.graph.iterNodes(e, d);
            return h
        };
        this.iterEdges = function (e, d) {
            b.graph.iterEdges(e, d);
            return h
        };
        this.getNodes = function (e) {
            return b.graph.getNodes(e)
        };
        this.getEdges = function (e) {
            return b.graph.getEdges(e)
        };
        this.activateMonitoring = function () {
            return b.monitor.activate()
        };
        this.desactivateMonitoring = function () {
            return b.monitor.desactivate()
        };
        b.bind("downnodes upnodes downgraph upgraph", function (b) {
            h.dispatch(b.type, b.content)
        });
        b.graph.bind("overnodes outnodes", function (b) {
            h.dispatch(b.type, b.content)
        })
    }
    var x = 0;
    i = void 0;
    i = [];
    sigma.init = function (b) {
        b = new t(b, (++x).toString());
        sigma.instances[x] = new p(b);
        return sigma.instances[x]
    };
    sigma.addPlugin = function (b, h, e) {
        p.prototype[b] = h;
        i.push(e)
    };
    sigma.chronos = new function () {
        function b(a) {
            window.setTimeout(a, 0);
            return f
        }
        function h() {
            for (f.dispatch("frameinserted"); m && r.length && e(););
            !m || !r.length ? a() : (w = (new Date).getTime(), q++, C = u - p, t = p - C, f.dispatch("insertframe"), b(h))
        }
        function e() {
            y %= r.length;
            if (!r[y].task()) {
                var a = r[y].taskName;
                v = v.filter(function (b) {
                    b.taskParent == a && r.push({
                        taskName: b.taskName,
                        task: b.task
                    });
                    return b.taskParent != a
                });
                f.dispatch("killed", r.splice(y--, 1)[0])
            }
            y++;
            u = (new Date).getTime() - w;
            return u <= t
        }
        function d() {
            m = !0;
            q = y = 0;
            x = w = (new Date).getTime();
            f.dispatch("start");
            f.dispatch("insertframe");
            b(h);
            return f
        }
        function a() {
            f.dispatch("stop");
            m = !1;
            return f
        }
        function g(a, b, c) {
            if ("function" != typeof a) throw Error('Task "' + b + '" is not a function');
            r.push({
                taskName: b,
                task: a
            });
            m = !(!m && !(c && d() || 1));
            return f
        }
        function c(a) {
            return a ? Object.keys(o).filter(function (a) {
                return !!o[a].on
            }).length : Object.keys(o).length
        }
        function i() {
            Object.keys(o).length ? (f.dispatch("startgenerators"), f.unbind("killed", j), b(function () {
                for (var a in o) o[a].on = !0, g(o[a].task, a, !1)
            }), f.bind("killed", j).runTasks()) : f.dispatch("stopgenerators");
            return f
        }
        function j(a) {
            if (void 0 != o[a.content.taskName]) o[a.content.taskName].del || !o[a.content.taskName].condition() ? delete o[a.content.taskName] : o[a.content.taskName].on = !1, 0 == c(!0) && i()
        }
        sigma.classes.EventDispatcher.call(this);
        var f = this,
            m = !1,
            l = 80,
            n = 0,
            q = 0,
            p = 1E3 / l,
            t = p,
            u = 0,
            x = 0,
            w = 0,
            C = 0,
            o = {},
            r = [],
            v = [],
            y = 0;
        this.frequency = function (a) {
            return void 0 != a ? (l = Math.abs(1 * a), p = 1E3 / l, q = 0, f) : l
        };
        this.runTasks = d;
        this.stopTasks = a;
        this.insertFrame = b;
        this.addTask = g;
        this.queueTask = function (a, b, c) {
            if ("function" != typeof a) throw Error('Task "' + b + '" is not a function');
            if (!r.concat(v).some(function (a) {
                return a.taskName == c
            })) throw Error('Parent task "' + c + '" of "' + b + '" is not attached.');
            v.push({
                taskParent: c,
                taskName: b,
                task: a
            });
            return f
        };
        this.removeTask = function (b, c) {
            if (void 0 == b) r = [], 1 == c ? v = [] : 2 == c && (r = v, v = []), a();
            else {
                var d = "string" == typeof b ? b : "";
                r = r.filter(function (a) {
                    return ("string" == typeof b ? a.taskName == b : a.task == b) ? (d = a.taskName, !1) : !0
                });
                0 < c && (v = v.filter(function (a) {
                    1 == c && a.taskParent == d && r.push(a);
                    return a.taskParent != d
                }))
            }
            m = !(r.length && (!a() || 1));
            return f
        };
        this.addGenerator = function (a, b, d) {
            if (void 0 != o[a]) return f;
            o[a] = {
                task: b,
                condition: d
            };
            0 == c(!0) && i();
            return f
        };
        this.removeGenerator = function (a) {
            if (o[a]) o[a].on = !1, o[a].del = !0;
            return f
        };
        this.startGenerators = i;
        this.getGeneratorsIDs = function () {
            return Object.keys(o)
        };
        this.getFPS = function () {
            m && (n = Math.round(1E4 * (q / ((new Date).getTime() - x))) / 10);
            return n
        };
        this.getTasksCount = function () {
            return r.length
        };
        this.getQueuedTasksCount = function () {
            return v.length
        };
        this.getExecutionTime = function () {
            return w - x
        };
        return this
    };
    sigma.debugMode = 0;
    sigma.log = function () {
        if (1 == sigma.debugMode) for (var b in arguments) console.log(arguments[b]);
        else if (1 < sigma.debugMode) for (b in arguments) throw Error(arguments[b]);
        return sigma
    };
    sigma.easing = {
        linear: {},
        quadratic: {}
    };
    sigma.easing.linear.easenone = function (b) {
        return b
    };
    sigma.easing.quadratic.easein = function (b) {
        return b * b
    };
    sigma.easing.quadratic.easeout = function (b) {
        return -b * (b - 2)
    };
    sigma.easing.quadratic.easeinout = function (b) {
        return 1 > (b *= 2) ? 0.5 * b * b : -0.5 * (--b * (b - 2) - 1)
    };
    sigma.tools.drawRoundRect = function (b, h, e, d, a, g, c) {
        var g = g ? g : 0,
            i = c ? c : [],
            i = "string" == typeof i ? i.split(" ") : i,
            c = g && (0 <= i.indexOf("topleft") || 0 <= i.indexOf("top") || 0 <= i.indexOf("left")),
            j = g && (0 <= i.indexOf("topright") || 0 <= i.indexOf("top") || 0 <= i.indexOf("right")),
            f = g && (0 <= i.indexOf("bottomleft") || 0 <= i.indexOf("bottom") || 0 <= i.indexOf("left")),
            i = g && (0 <= i.indexOf("bottomright") || 0 <= i.indexOf("bottom") || 0 <= i.indexOf("right"));
        b.moveTo(h, e + g);
        c ? b.arcTo(h, e, h + g, e, g) : b.lineTo(h, e);
        j ? (b.lineTo(h + d - g, e), b.arcTo(h + d, e, h + d, e + g, g)) : b.lineTo(h + d, e);
        i ? (b.lineTo(h + d, e + a - g), b.arcTo(h + d, e + a, h + d - g, e + a, g)) : b.lineTo(h + d, e + a);
        f ? (b.lineTo(h + g, e + a), b.arcTo(h, e + a, h, e + a - g, g)) : b.lineTo(h, e + a);
        b.lineTo(h, e + g)
    };
    sigma.tools.getRGB = function (b, g) {
        var b = b.toString(),
            e = {
                r: 0,
                g: 0,
                b: 0
            };
        if (3 <= b.length && "#" == b.charAt(0)) {
            var d = b.length - 1;
            6 == d ? e = {
                r: parseInt(b.charAt(1) + b.charAt(2), 16),
                g: parseInt(b.charAt(3) + b.charAt(4), 16),
                b: parseInt(b.charAt(5) + b.charAt(5), 16)
            } : 3 == d && (e = {
                r: parseInt(b.charAt(1) + b.charAt(1), 16),
                g: parseInt(b.charAt(2) + b.charAt(2), 16),
                b: parseInt(b.charAt(3) + b.charAt(3), 16)
            })
        }
        g && (e = [e.r, e.g, e.b]);
        return e
    };
    sigma.tools.rgbToHex = function (b, g, e) {
        return sigma.tools.toHex(b) + sigma.tools.toHex(g) + sigma.tools.toHex(e)
    };
    sigma.tools.toHex = function (b) {
        b = parseInt(b, 10);
        if (isNaN(b)) return "00";
        b = Math.max(0, Math.min(b, 255));
        return "0123456789ABCDEF".charAt((b - b % 16) / 16) + "0123456789ABCDEF".charAt(b % 16)
    };
    sigma.publicPrototype = p.prototype
})();
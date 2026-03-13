import { useRef, useEffect } from 'react';
import * as THREE from 'three';

const NODE_COUNT = 80;
const CONNECTION_DISTANCE = 2.8;
const BOUNDS = 8;

interface NodeData {
  position: THREE.Vector3;
  velocity: THREE.Vector3;
  baseColor: THREE.Color;
}

export default function NetworkAnimation() {
  const containerRef = useRef<HTMLDivElement>(null);
  const frameRef = useRef<number>(0);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    // ── Scene setup ──────────────────────────────────
    const scene = new THREE.Scene();
    scene.fog = new THREE.FogExp2(0x000000, 0.06);

    const camera = new THREE.PerspectiveCamera(60, window.innerWidth / window.innerHeight, 0.1, 100);
    camera.position.set(0, 0, 14);

    const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: true });
    renderer.setSize(window.innerWidth, window.innerHeight);
    renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    renderer.setClearColor(0x000000, 1);
    container.appendChild(renderer.domElement);

    // ── Colors (match theme) ─────────────────────────
    const cyan = new THREE.Color(0x00e5ff);
    const violet = new THREE.Color(0x8b5cf6);
    const green = new THREE.Color(0x22c55e);
    const colorPalette = [cyan, violet, green, cyan, cyan]; // Weighted toward cyan

    // ── Nodes ────────────────────────────────────────
    const nodes: NodeData[] = [];
    const nodeGeometry = new THREE.SphereGeometry(0.06, 12, 12);

    for (let i = 0; i < NODE_COUNT; i++) {
      const color = colorPalette[Math.floor(Math.random() * colorPalette.length)];
      const nodeMaterial = new THREE.MeshBasicMaterial({
        color,
        transparent: true,
        opacity: 0.9,
      });
      const mesh = new THREE.Mesh(nodeGeometry, nodeMaterial);

      const pos = new THREE.Vector3(
        (Math.random() - 0.5) * BOUNDS * 2,
        (Math.random() - 0.5) * BOUNDS * 2,
        (Math.random() - 0.5) * BOUNDS * 1.5,
      );
      mesh.position.copy(pos);
      scene.add(mesh);

      // Glow sprite
      const spriteMaterial = new THREE.SpriteMaterial({
        color,
        transparent: true,
        opacity: 0.3,
        blending: THREE.AdditiveBlending,
      });
      const sprite = new THREE.Sprite(spriteMaterial);
      sprite.scale.set(0.5, 0.5, 1);
      mesh.add(sprite);

      nodes.push({
        position: pos,
        velocity: new THREE.Vector3(
          (Math.random() - 0.5) * 0.005,
          (Math.random() - 0.5) * 0.005,
          (Math.random() - 0.5) * 0.003,
        ),
        baseColor: color,
      });
    }

    // ── Edges (dynamic lines) ────────────────────────
    const linePositions = new Float32Array(NODE_COUNT * NODE_COUNT * 6);
    const lineColors = new Float32Array(NODE_COUNT * NODE_COUNT * 6);
    const lineGeometry = new THREE.BufferGeometry();
    lineGeometry.setAttribute('position', new THREE.BufferAttribute(linePositions, 3));
    lineGeometry.setAttribute('color', new THREE.BufferAttribute(lineColors, 3));
    lineGeometry.setDrawRange(0, 0);

    const lineMaterial = new THREE.LineBasicMaterial({
      vertexColors: true,
      transparent: true,
      opacity: 0.35,
      blending: THREE.AdditiveBlending,
    });
    const linesMesh = new THREE.LineSegments(lineGeometry, lineMaterial);
    scene.add(linesMesh);

    // ── Pulse particles (data transfer effect) ───────
    const pulseGeometry = new THREE.SphereGeometry(0.03, 6, 6);
    const pulseMaterial = new THREE.MeshBasicMaterial({
      color: cyan,
      transparent: true,
      opacity: 0.8,
      blending: THREE.AdditiveBlending,
    });

    interface PulseParticle {
      mesh: THREE.Mesh;
      from: THREE.Vector3;
      to: THREE.Vector3;
      progress: number;
      speed: number;
    }

    const pulses: PulseParticle[] = [];
    let pulseTimer = 0;

    // ── Animate ──────────────────────────────────────
    const clock = new THREE.Clock();

    function animate() {
      frameRef.current = requestAnimationFrame(animate);
      const elapsed = clock.getElapsedTime();
      const delta = clock.getDelta();

      // Move nodes
      for (let i = 0; i < NODE_COUNT; i++) {
        const node = nodes[i];
        node.position.add(node.velocity);

        // Bounce off bounds
        ['x', 'y', 'z'].forEach((axis) => {
          const a = axis as 'x' | 'y' | 'z';
          const limit = a === 'z' ? BOUNDS * 0.75 : BOUNDS;
          if (Math.abs(node.position[a]) > limit) {
            node.velocity[a] *= -1;
            node.position[a] = Math.sign(node.position[a]) * limit;
          }
        });

        // Update mesh position (nodes are scene children by index: first NODE_COUNT)
        const mesh = scene.children[i] as THREE.Mesh;
        mesh.position.copy(node.position);
      }

      // Update connections
      let lineIndex = 0;
      const posAttr = lineGeometry.getAttribute('position') as THREE.BufferAttribute;
      const colAttr = lineGeometry.getAttribute('color') as THREE.BufferAttribute;

      for (let i = 0; i < NODE_COUNT; i++) {
        for (let j = i + 1; j < NODE_COUNT; j++) {
          const dist = nodes[i].position.distanceTo(nodes[j].position);
          if (dist < CONNECTION_DISTANCE) {
            const alpha = 1 - dist / CONNECTION_DISTANCE;
            const idx = lineIndex * 6;

            posAttr.array[idx] = nodes[i].position.x;
            posAttr.array[idx + 1] = nodes[i].position.y;
            posAttr.array[idx + 2] = nodes[i].position.z;
            posAttr.array[idx + 3] = nodes[j].position.x;
            posAttr.array[idx + 4] = nodes[j].position.y;
            posAttr.array[idx + 5] = nodes[j].position.z;

            // Blend colors
            const mixColor = nodes[i].baseColor.clone().lerp(nodes[j].baseColor, 0.5);
            colAttr.array[idx] = mixColor.r * alpha;
            colAttr.array[idx + 1] = mixColor.g * alpha;
            colAttr.array[idx + 2] = mixColor.b * alpha;
            colAttr.array[idx + 3] = mixColor.r * alpha;
            colAttr.array[idx + 4] = mixColor.g * alpha;
            colAttr.array[idx + 5] = mixColor.b * alpha;

            lineIndex++;
          }
        }
      }

      lineGeometry.setDrawRange(0, lineIndex * 2);
      posAttr.needsUpdate = true;
      colAttr.needsUpdate = true;

      // Spawn pulses
      pulseTimer += delta;
      if (pulseTimer > 0.3 && pulses.length < 15) {
        pulseTimer = 0;
        // Find a random connected pair
        const i = Math.floor(Math.random() * NODE_COUNT);
        for (let j = 0; j < NODE_COUNT; j++) {
          if (i !== j && nodes[i].position.distanceTo(nodes[j].position) < CONNECTION_DISTANCE) {
            const mesh = new THREE.Mesh(pulseGeometry, pulseMaterial.clone());
            scene.add(mesh);
            pulses.push({
              mesh,
              from: nodes[i].position.clone(),
              to: nodes[j].position.clone(),
              progress: 0,
              speed: 0.8 + Math.random() * 1.2,
            });
            break;
          }
        }
      }

      // Update pulses
      for (let i = pulses.length - 1; i >= 0; i--) {
        const p = pulses[i];
        p.progress += delta * p.speed;
        if (p.progress >= 1) {
          scene.remove(p.mesh);
          pulses.splice(i, 1);
        } else {
          p.mesh.position.lerpVectors(p.from, p.to, p.progress);
          (p.mesh.material as THREE.MeshBasicMaterial).opacity = Math.sin(p.progress * Math.PI) * 0.9;
        }
      }

      // Camera orbit
      camera.position.x = Math.sin(elapsed * 0.08) * 14;
      camera.position.z = Math.cos(elapsed * 0.08) * 14;
      camera.position.y = Math.sin(elapsed * 0.05) * 2;
      camera.lookAt(0, 0, 0);

      renderer.render(scene, camera);
    }

    animate();

    // ── Resize ───────────────────────────────────────
    const handleResize = () => {
      camera.aspect = window.innerWidth / window.innerHeight;
      camera.updateProjectionMatrix();
      renderer.setSize(window.innerWidth, window.innerHeight);
    };
    window.addEventListener('resize', handleResize);

    // ── Cleanup ──────────────────────────────────────
    return () => {
      window.removeEventListener('resize', handleResize);
      cancelAnimationFrame(frameRef.current);
      renderer.dispose();
      if (container.contains(renderer.domElement)) {
        container.removeChild(renderer.domElement);
      }
    };
  }, []);

  return (
    <div
      ref={containerRef}
      style={{
        position: 'fixed',
        top: 0,
        left: 0,
        width: '100vw',
        height: '100vh',
        zIndex: 0,
        pointerEvents: 'none',
      }}
    />
  );
}
